from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceLogin
from requests_oauthlib import OAuth2Session
from typing import Dict, Optional, Union, List, TYPE_CHECKING, Any, TypeVar, Literal
from pyspark.sql import DataFrame
from .utils.logging_utils import setup_logging
from .utils.spark_utils import initialize_spark
from .cache.field_cache import FieldCache
from .operations.extract import DataExtractor
from .operations.load import DataLoader
from .utils.glue_utils import get_glue_spark_session
from .utils.secrets_utils import SecretsManager
import secrets
import hashlib
import base64
from oauthlib.oauth2.rfc6749.errors import OAuth2Error
from .utils.token_manager import TokenManager
import jwt
import requests
import time
import importlib
from .config import SalesforceConfig, AWSConfig, ProcessingConfig


# Conditional imports for type hints
if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from awsglue.dynamicframe import DynamicFrame
    import duckdb

T = TypeVar('T')

class ScalableSalesforceConnector:
    def __init__(self,
                 sf_config: Optional[SalesforceConfig] = None,
                 aws_config: Optional[AWSConfig] = None,
                 processing_config: Optional[ProcessingConfig] = None):
        """
        Initialize the Salesforce connector with configuration objects.
        
        Args:
            sf_config: Salesforce connection configuration
            aws_config: AWS services configuration
            processing_config: Data processing configuration
        """
        # Initialize with default configs if not provided
        self.sf_config = sf_config or SalesforceConfig()
        self.aws_config = aws_config or AWSConfig()
        self.proc_config = processing_config or ProcessingConfig()
        
        self.logger = setup_logging()
        self.version = self.sf_config.version
        self.sf = None  # Initialize sf as None first
        
        # Initialize Spark
        self.spark = None
        try:
            if self.proc_config.require_spark or self.proc_config.glue_context:
                if self.proc_config.glue_context:
                    self.spark = get_glue_spark_session(
                        self.proc_config.glue_context,
                        self.proc_config.spark_config
                    )
                else:
                    self.spark = initialize_spark(self.proc_config.spark_config)
                    
                if self.spark is None and self.proc_config.require_spark:
                    raise Exception("Failed to initialize Spark session")
                    
        except Exception as e:
            self.logger.warning(f"Spark initialization failed: {str(e)}")
            if self.proc_config.require_spark:
                raise
            self.spark = None
            
        self.cache = FieldCache(self.proc_config.cache_size)

        # Get credentials and initialize Salesforce connection
        credentials = None
        if self.aws_config.secret_name:
            self.logger.info(f"Retrieving credentials from Secrets Manager: {self.aws_config.secret_name}")
            try:
                secrets_manager = SecretsManager(self.aws_config.region_name)
                credentials = secrets_manager.get_secret(self.aws_config.secret_name)
            except Exception as e:
                self.logger.error(f"Failed to retrieve credentials from Secrets Manager: {str(e)}")
                raise
        elif self.aws_config.secret_values:
            self.logger.info(f"Using provided secret values for {self.sf_config.auth_method} authentication")
            credentials = self.aws_config.secret_values
        
        if credentials:
            # Handle authentication based on method
            if self.sf_config.auth_method == 'jwt_secret':
                self._handle_jwt_secret_auth(credentials)
            elif self.sf_config.auth_method == 'jwt_cert':
                self._handle_jwt_cert_auth(
                    credentials.get('client_id', self.sf_config.client_id),
                    credentials.get('username', self.sf_config.username),
                    credentials.get('private_key', self.sf_config.private_key),
                    credentials.get('private_key_path', self.sf_config.private_key_path)
                )
            elif self.sf_config.auth_method == 'oauth':
                self.token_manager = TokenManager(
                    client_id=credentials.get('client_id', self.sf_config.client_id),
                    client_secret=credentials.get('client_secret', self.sf_config.client_secret),
                    token_url=credentials.get('token_url', self.sf_config.token_url)
                )
                if 'existing_token' in credentials:
                    self.token_manager.set_token(credentials['existing_token'])
                self._handle_oauth_pkce_auth(
                    credentials.get('client_id', self.sf_config.client_id),
                    credentials.get('client_secret', self.sf_config.client_secret),
                    credentials.get('redirect_uri', self.sf_config.redirect_uri),
                    credentials.get('auth_url', self.sf_config.auth_url),
                    credentials.get('token_url', self.sf_config.token_url)
                )
            elif self.sf_config.auth_method == 'password':
                self._handle_password_auth(
                    credentials.get('username', self.sf_config.username),
                    credentials.get('password', self.sf_config.password),
                    credentials.get('security_token', self.sf_config.security_token),
                    credentials.get('domain', self.sf_config.domain)
                )
        else:
            # Use direct configuration if no credentials from secrets
            if self.sf_config.auth_method == 'oauth':
                if not all([self.sf_config.client_id, self.sf_config.client_secret, self.sf_config.token_url]):
                    raise ValueError("OAuth2 requires client_id, client_secret, and token_url")
                
                self.token_manager = TokenManager(
                    client_id=self.sf_config.client_id,
                    client_secret=self.sf_config.client_secret,
                    token_url=self.sf_config.token_url
                )
                
                # Handle OAuth PKCE flow
                if not all([self.sf_config.redirect_uri, self.sf_config.auth_url]):
                    raise ValueError("OAuth2 PKCE requires redirect_uri and auth_url for authentication")
                
                self._handle_oauth_pkce_auth(
                    self.sf_config.client_id,
                    self.sf_config.client_secret,
                    self.sf_config.redirect_uri,
                    self.sf_config.auth_url,
                    self.sf_config.token_url
                )
            elif self.sf_config.auth_method == 'password':
                if not all([self.sf_config.username, self.sf_config.password, self.sf_config.security_token]):
                    raise ValueError("Password auth requires username, password, and security_token")
                self._handle_password_auth(
                    self.sf_config.username,
                    self.sf_config.password,
                    self.sf_config.security_token,
                    self.sf_config.domain
                )
            elif self.sf_config.auth_method == 'jwt_cert':
                if not all([self.sf_config.client_id, self.sf_config.username]) or \
                   not (self.sf_config.private_key or self.sf_config.private_key_path):
                    raise ValueError("JWT cert auth requires client_id, username, and either private_key or private_key_path")
                self._handle_jwt_cert_auth(
                    self.sf_config.client_id,
                    self.sf_config.username,
                    self.sf_config.private_key,
                    self.sf_config.private_key_path
                )
            elif self.sf_config.auth_method == 'jwt_secret':
                raise ValueError("JWT secret auth requires credentials to be provided")

        # Initialize DuckDB connection if Spark is not available
        self.duckdb_conn = None
        if self.proc_config.require_duckdb:
            try:
                duckdb = importlib.import_module('duckdb')
                self.duckdb_conn = duckdb.connect(database=':memory:')
                self.duckdb_conn.execute(f"SET memory_limit='{self.proc_config.duckdb_memory_limit}'")
                self.duckdb_conn.execute("SET threads TO 4")
                self.logger.info("Successfully initialized DuckDB connection")
            except ImportError:
                self.logger.error("DuckDB not installed. Please install with: pip install duckdb")
                if self.proc_config.require_duckdb:
                    raise
            except Exception as e:
                self.logger.error(f"Failed to initialize DuckDB: {str(e)}")
                if self.proc_config.require_duckdb:
                    raise


        # Initialize extractor and loader only after sf is initialized
        if self.sf is None:
            raise ValueError("Failed to initialize Salesforce connection")

        self.extractor = DataExtractor(self.sf, self.spark, self.cache, self.logger)
        self.loader = DataLoader(self.sf, self.logger)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Explicitly close resources."""
        if hasattr(self, 'duckdb_conn') and self.duckdb_conn:
            self.duckdb_conn.close()
            self.duckdb_conn = None
        if hasattr(self, 'spark') and self.spark:
            self.spark.stop()
            self.spark = None
        if hasattr(self, 'sf'):
            self.sf = None  # Release Salesforce connection
        if hasattr(self, 'token_manager'):
            self.token_manager = None  # Release token manager

    def _ensure_valid_connection(self):
        """Ensure connection is valid, refreshing token if necessary."""
        try:
            # Test connection
            self.sf.query("SELECT Id FROM Account LIMIT 1")
        except Exception as e:
            self.logger.warning(f"Connection test failed: {str(e)}")
            # Try to refresh token
            token = self.token_manager.get_valid_token()
            self.sf = Salesforce(
                instance_url=token['instance_url'],
                session_id=token['access_token'],
                version=self.version
            )

    def extract_data(
        self, 
        query: str, 
        partition_field: Optional[str] = None,
        num_partitions: int = 10,
        output_format: Literal['auto', 'spark', 'duckdb', 'dict', 'dynamicframe'] = 'auto'
    ) -> Union[List[Dict[str, Any]], 'SparkDataFrame', 'DynamicFrame', 'duckdb.DuckDBPyRelation']:
        """Extract data with automatic format conversion based on context."""
        try:
            self._ensure_valid_connection()
            
            # Use DataExtractor for extraction
            raw_data = self.extractor.extract_data(
                query=query,
                partition_field=partition_field,
                num_partitions=num_partitions
            )

            # Determine output format
            if output_format == 'auto':
                if hasattr(self, 'proc_config') and self.proc_config.glue_context:
                    output_format = 'dynamicframe'
                elif self.spark:
                    output_format = 'spark'
                elif self.duckdb_conn:
                    output_format = 'duckdb'
                else:
                    output_format = 'dict'
                    self.logger.info("No processing engine available. Returning dictionary.")

            # Format-specific conversions
            try:
                if output_format == 'dict':
                    return raw_data if isinstance(raw_data, list) else raw_data.collect()
                
                elif output_format == 'duckdb':
                    if not self.duckdb_conn:
                        raise RuntimeError("DuckDB connection not available. Set require_duckdb=True in ProcessingConfig")
                    
                    table_name = f"sf_data_{abs(hash(query))}"
                    self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    
                    try:
                        if self.spark:
                            # Convert Spark DataFrame to pandas
                            spark_df = raw_data if isinstance(raw_data, DataFrame) else self.spark.createDataFrame(raw_data)
                            pandas_df = spark_df.toPandas()
                        else:
                            # Direct pandas conversion
                            pd = importlib.import_module('pandas')
                            records = raw_data if isinstance(raw_data, list) else raw_data.collect()
                            pandas_df = pd.DataFrame.from_records(records)
                        
                        # Create DuckDB table from pandas DataFrame
                        self.duckdb_conn.execute(
                            f"CREATE TABLE {table_name} AS SELECT * FROM pandas_df"
                        )
                        return self.duckdb_conn.table(table_name)
                        
                    except Exception as e:
                        self.logger.error(f"Failed to convert data to DuckDB table: {str(e)}")
                        raise RuntimeError(f"Failed to convert to {output_format} format") from e
                
                elif output_format == 'spark':
                    if not self.spark:
                        raise RuntimeError("Spark session not available")
                    return raw_data if isinstance(raw_data, DataFrame) else self.spark.createDataFrame(raw_data)
                
                elif output_format == 'dynamicframe':
                    if not hasattr(self, 'proc_config') or not self.proc_config.glue_context:
                        raise RuntimeError("Glue context not available")
                    from awsglue.dynamicframe import DynamicFrame
                    spark_df = raw_data if isinstance(raw_data, DataFrame) else self.spark.createDataFrame(raw_data)
                    return DynamicFrame.fromDF(spark_df, self.proc_config.glue_context, "extracted_data")
                
                else:
                    raise ValueError(f"Unsupported output format: {output_format}")

            except Exception as e:
                self.logger.error(f"Format conversion failed: {str(e)}")
                raise RuntimeError(f"Failed to convert to {output_format} format") from e

        except Exception as e:
            self.logger.error(f"Data extraction failed: {str(e)}")
            raise

    def query_data(self, sql_query: str):
        """
        Execute SQL query on the extracted data using DuckDB.
        
        Args:
            sql_query: SQL query to execute
            
        Returns:
            DuckDB result relation
        """
        if self.duckdb_conn is None:
            raise ValueError("DuckDB connection not initialized")
        
        try:
            return self.duckdb_conn.execute(sql_query).fetch_df()
        except Exception as e:
            self.logger.error(f"DuckDB query failed: {str(e)}")
            raise

    def load_data(self, object_name: str, data, operation: str = 'insert', 
                 batch_size: int = 10000):
        """Load data with automatic token refresh."""
        self._ensure_valid_connection()
        return self.loader.load_data(object_name, data, operation, batch_size)

    def clear_caches(self):
        self.cache.clear() 

    def _handle_jwt_secret_auth(self, credentials: Dict):
        """Handle JWT authentication with pre-generated token."""
        self.logger.info("Using JWT Bearer flow with pre-generated token")
        try:
            request_params = {
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": credentials['auth_token']
            }

            response = requests.post(
                url=f"{credentials['host']}/services/oauth2/token",
                data=request_params,
                headers={
                    'content-type': 'application/x-www-form-urlencoded',
                    'accept': 'application/json'
                }
            )

            if response.status_code != 200:
                raise Exception(f"Token request failed: {response.text}")

            token = response.json()
            self.sf = Salesforce(
                instance_url=token['instance_url'],
                session_id=token['access_token'],
                version=self.version
            )
            self.logger.info("Successfully connected using JWT Bearer flow")
            
        except Exception as e:
            self.logger.error(f"Failed to authenticate using JWT Bearer flow: {str(e)}")
            raise

    def _handle_jwt_cert_auth(self, client_id: str, username: str, private_key: Optional[str], private_key_path: Optional[str]):
        """Handle JWT authentication with certificate."""
        self.logger.info("Using JWT Bearer flow with certificate")
        try:
            # Get private key
            if private_key:
                key = private_key
            elif private_key_path:
                with open(private_key_path, 'r') as f:
                    key = f.read()
            else:
                raise ValueError("Must provide either private_key or private_key_path for certificate-based JWT authentication")

            # Generate JWT token
            jwt_payload = {
                'exp': int(time.time()) + 300,
                'iss': client_id,
                'sub': username,
                'aud': 'https://login.salesforce.com'
            }

            assertion = jwt.encode(
                jwt_payload,
                key,
                algorithm='RS256'
            )

            request_params = {
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion
            }

            response = requests.post(
                'https://login.salesforce.com/services/oauth2/token',
                data=request_params
            )

            if response.status_code != 200:
                raise Exception(f"Token request failed: {response.text}")

            token = response.json()
            self.sf = Salesforce(
                instance_url=token['instance_url'],
                session_id=token['access_token'],
                version=self.version
            )
            self.logger.info("Successfully connected using certificate-based JWT flow")
            
        except Exception as e:
            self.logger.error(f"Failed to authenticate using certificate-based JWT flow: {str(e)}")
            raise

    def _handle_oauth_pkce_auth(self, client_id: str, client_secret: str, redirect_uri: str, auth_url: str, token_url: str):
        """Handle OAuth2 authentication with PKCE."""
        self.logger.info("Using OAuth2 with PKCE for authentication")
        try:
            # Generate PKCE code verifier and challenge
            code_verifier = secrets.token_urlsafe(96)
            code_challenge = base64.urlsafe_b64encode(
                hashlib.sha256(code_verifier.encode('ascii')).digest()
            ).rstrip(b'=').decode('ascii')

            # Define scopes
            scopes = ['api', 'refresh_token']

            oauth = OAuth2Session(
                client_id,
                redirect_uri=redirect_uri,
                scope=scopes
            )

            # Add PKCE parameters
            authorization_url, state = oauth.authorization_url(
                auth_url,
                code_challenge=code_challenge,
                code_challenge_method='S256'
            )

            print(f"Please go to {authorization_url} and authorize access.")
            authorization_response = input("Enter the full callback URL: ")

            try:
                # Exchange authorization code for token with PKCE verifier
                token = oauth.fetch_token(
                    token_url,
                    authorization_response=authorization_response,
                    client_secret=client_secret,
                    code_verifier=code_verifier,
                    include_client_id=True
                )

                self.sf = Salesforce(
                    instance_url=token['instance_url'],
                    session_id=token['access_token'],
                    version=self.version
                )
                self.logger.info("Successfully connected to Salesforce using OAuth2")

            except Warning as w:
                # Handle scope change warning but continue if we got the token
                self.logger.warning(f"Scope warning: {str(w)}")
                if not oauth.token:
                    raise
                self.sf = Salesforce(
                    instance_url=oauth.token['instance_url'],
                    session_id=oauth.token['access_token'],
                    version=self.version
                )
                self.logger.info("Successfully connected despite scope warning")

            # Save the new token
            if hasattr(self, 'token_manager'):
                self.token_manager.set_token(token)

        except Exception as e:
            self.logger.error(f"Failed to authenticate using OAuth2: {str(e)}")
            raise

    def _handle_password_auth(self, username: str, password: str, security_token: str, domain: str = 'login'):
        """Handle password-based authentication."""
        self.logger.info("Using password authentication")
        try:
            self.sf = Salesforce(
                username=username,
                password=password,
                security_token=security_token,
                domain=domain,
                version=self.version
            )
            self.logger.info("Successfully connected using password authentication")
        except Exception as e:
            self.logger.error(f"Failed to connect using password authentication: {str(e)}")
            raise

    def __del__(self):
        """Cleanup resources."""
        if hasattr(self, 'duckdb_conn') and self.duckdb_conn:
            self.duckdb_conn.close()

    def is_connected(self) -> bool:
        """Check if the connection to Salesforce is active."""
        try:
            self.sf.query("SELECT Id FROM Account LIMIT 1")
            return True
        except Exception as e:
            self.logger.debug(f"Connection check failed: {str(e)}")
            return False

    def _clean_salesforce_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean a Salesforce record by removing unwanted attributes."""
        if isinstance(record, dict):
            cleaned = {}
            for key, value in record.items():
                if key not in ['attributes']:  # Skip Salesforce metadata
                    if isinstance(value, dict):
                        cleaned[key] = self._clean_salesforce_record(value)
                    else:
                        cleaned[key] = value
            return cleaned
        return record