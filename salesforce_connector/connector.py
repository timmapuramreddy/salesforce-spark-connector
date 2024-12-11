from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceLogin
from requests_oauthlib import OAuth2Session
from typing import Dict, Optional
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

class ScalableSalesforceConnector:
    def __init__(self, 
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 security_token: Optional[str] = None,
                 private_key: Optional[str] = None,
                 private_key_path: Optional[str] = None,
                 secret_name: Optional[str] = None,
                 region_name: Optional[str] = None,
                 redirect_uri: Optional[str] = None,
                 auth_url: Optional[str] = None,
                 token_url: Optional[str] = None,
                 auth_method: str = 'oauth',
                 secret_values: Optional[Dict] = None,
                 spark_config: Optional[Dict[str, str]] = None,
                 domain: str = 'login',
                 version: str = '54.0',
                 cache_size: int = 100,
                 glue_context = None):
        """
        Initialize the Salesforce connector with either direct credentials or OAuth2.
        
        Args:
            client_id: Salesforce client ID (for OAuth2)
            client_secret: Salesforce client secret (for OAuth2)
            username: Salesforce username (for direct credentials)
            password: Salesforce password (for direct credentials)
            security_token: Salesforce security token (for direct credentials)
            secret_name: AWS Secrets Manager secret name/ARN (optional)
            region_name: AWS region for Secrets Manager (optional)
            redirect_uri: Redirect URI for OAuth2
            auth_url: Authorization URL for OAuth2
            token_url: Token URL for OAuth2
            spark_config: Additional Spark configuration
            domain: Salesforce domain
            version: API version
            cache_size: Size of the field type cache
            glue_context: AWS Glue context (optional)
        """
        
        self.logger = setup_logging()
        
        # Initialize Spark
        if glue_context:
            self.spark = get_glue_spark_session(glue_context, spark_config)
        else:
            self.spark = initialize_spark(spark_config)
        
        self.cache = FieldCache(cache_size)

        # Handle direct secret values for all auth methods
        if secret_values:
            self.logger.info(f"Using provided secret values for {auth_method} authentication")
            if auth_method == 'jwt_secret':
                self._handle_jwt_secret_auth(secret_values)
            elif auth_method == 'jwt_cert':
                self._handle_jwt_cert_auth(
                    secret_values.get('client_id'),
                    secret_values.get('username'),
                    secret_values.get('private_key'),
                    secret_values.get('private_key_path')
                )
            elif auth_method == 'oauth':
                # Initialize TokenManager with provided secrets
                self.token_manager = TokenManager(
                    client_id=secret_values.get('client_id'),
                    client_secret=secret_values.get('client_secret'),
                    token_url=secret_values.get('token_url')
                )
                if 'existing_token' in secret_values:
                    self.token_manager.set_token(secret_values['existing_token'])
                self._handle_oauth_pkce_auth(
                    secret_values.get('client_id'),
                    secret_values.get('client_secret'),
                    secret_values.get('redirect_uri'),
                    secret_values.get('auth_url'),
                    secret_values.get('token_url')
                )
            elif auth_method == 'password':
                self._handle_password_auth(
                    secret_values.get('username'),
                    secret_values.get('password'),
                    secret_values.get('security_token'),
                    secret_values.get('domain', 'login')
                )
            
            self.extractor = DataExtractor(self.sf, self.spark, self.cache, self.logger)
            self.loader = DataLoader(self.sf, self.logger)
            return

        # Initialize credentials from Secrets Manager if provided
        if secret_name:
            self.logger.info(f"Retrieving credentials from Secrets Manager: {secret_name}")
            try:
                secrets_manager = SecretsManager(region_name)
                credentials = secrets_manager.get_secret(secret_name)
                
                # Handle JWT secret authentication if specified
                if auth_method == 'jwt_secret':
                    self._handle_jwt_secret_auth(credentials)
                    self.extractor = DataExtractor(self.sf, self.spark, self.cache, self.logger)
                    self.loader = DataLoader(self.sf, self.logger)
                    return
                    
                # Otherwise, proceed with existing secret handling
                client_id = credentials.get('salesforce_client_id', client_id)
                client_secret = credentials.get('salesforce_client_secret', client_secret)
                username = credentials.get('salesforce_username', username)
                password = credentials.get('salesforce_password', password)
                security_token = credentials.get('salesforce_security_token', security_token)
            except Exception as e:
                self.logger.error(f"Failed to retrieve credentials from Secrets Manager: {str(e)}")
                raise

        # Handle certificate-based JWT authentication
        if auth_method == 'jwt_cert' and client_id and username:
            # JWT tokens are short-lived and need to be regenerated rather than refreshed
            # TokenManager is not used here because JWT auth generates a new token each time
            # using the private key certificate, rather than refreshing an existing token
            self._handle_jwt_cert_auth(client_id, username, private_key, private_key_path)
            self.extractor = DataExtractor(self.sf, self.spark, self.cache, self.logger)
            self.loader = DataLoader(self.sf, self.logger)
            return

        # Initialize TokenManager for OAuth2
        if client_id and client_secret and token_url:
            self.token_manager = TokenManager(
                client_id=client_id,
                client_secret=client_secret,
                token_url=token_url
            )
            
            # Try to reuse existing token
            try:
                token = self.token_manager.get_valid_token()
                self.sf = Salesforce(
                    instance_url=token['instance_url'],
                    session_id=token['access_token'],
                    version=version
                )
                self.logger.info("Successfully connected using existing token")
                self.extractor = DataExtractor(self.sf, self.spark, self.cache, self.logger)
                self.loader = DataLoader(self.sf, self.logger)
                return
            except Exception as e:
                self.logger.debug(f"Could not reuse token: {str(e)}")
                # Continue with new authentication if we have all required parameters
                if not all([redirect_uri, auth_url]):
                    raise ValueError(
                        "No valid token found and missing required OAuth2 parameters "
                        "(redirect_uri, auth_url) for new authentication"
                    )

        # OAuth2 Authentication with PKCE
        if client_id and client_secret and redirect_uri and auth_url and token_url:
            self.logger.info("Using OAuth2 for authentication")
            try:
                # Proceed with new authentication
                # Generate PKCE code verifier and challenge
                code_verifier = secrets.token_urlsafe(96)
                code_challenge = base64.urlsafe_b64encode(
                    hashlib.sha256(code_verifier.encode('ascii')).digest()
                ).rstrip(b'=').decode('ascii')

                # Define scopes
                scopes = ['api', 'refresh_token']  # Simplified scope list

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
                        version=version
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
                        version=version
                    )
                    self.logger.info("Successfully connected despite scope warning")

                # Save the new token
                self.token_manager.set_token(token)
                
            except Exception as e:
                self.logger.error(f"Failed to authenticate using OAuth2: {str(e)}")
                raise
        # Direct credentials authentication
        elif username and password and security_token:
            self.logger.info("Using direct credentials for authentication")
            try:
                self.sf = Salesforce(
                    username=username,
                    password=password,
                    security_token=security_token,
                    domain=domain,
                    version=version
                )
                self.logger.info("Successfully connected using direct credentials")
            except Exception as e:
                self.logger.error(f"Failed to connect to Salesforce: {str(e)}")
                raise
        else:
            raise ValueError(
                "Must provide either:\n"
                "1. OAuth2 credentials (client_id, client_secret, token_url) with existing valid token, or\n"
                "2. Full OAuth2 parameters for new authentication (client_id, client_secret, redirect_uri, auth_url, token_url), or\n"
                "3. Direct credentials (username, password, security_token)"
            )

        self.extractor = DataExtractor(self.sf, self.spark, self.cache, self.logger)
        self.loader = DataLoader(self.sf, self.logger)

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
                version=self.sf.version
            )

    def extract_data(self, query: str, partition_field: str = None, 
                    num_partitions: int = 10):
        """Extract data with automatic token refresh."""
        self._ensure_valid_connection()
        return self.extractor.extract_data(query, partition_field, num_partitions)

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