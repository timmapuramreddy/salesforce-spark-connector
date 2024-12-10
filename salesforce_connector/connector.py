from simple_salesforce import Salesforce
from typing import Dict, Optional
from .utils.logging_utils import setup_logging
from .utils.spark_utils import initialize_spark
from .cache.field_cache import FieldCache
from .operations.extract import DataExtractor
from .operations.load import DataLoader
from .utils.glue_utils import get_glue_spark_session
from .utils.secrets_utils import SecretsManager

class ScalableSalesforceConnector:
    def __init__(self, 
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 security_token: Optional[str] = None,
                 secret_name: Optional[str] = None,
                 region_name: Optional[str] = None,
                 spark_config: Optional[Dict[str, str]] = None,
                 domain: str = 'login',
                 version: str = '54.0',
                 cache_size: int = 100,
                 glue_context = None):
        """
        Initialize the Salesforce connector with either direct credentials or AWS Secrets Manager.
        
        Args:
            username: Salesforce username (for direct credentials)
            password: Salesforce password (for direct credentials)
            security_token: Salesforce security token (for direct credentials)
            secret_name: AWS Secrets Manager secret name/ARN (optional)
            region_name: AWS region for Secrets Manager (optional)
            spark_config: Additional Spark configuration
            domain: Salesforce domain
            version: API version
            cache_size: Size of the field type cache
            glue_context: AWS Glue context (optional)
            
        Examples:
            # Using direct credentials
            connector = ScalableSalesforceConnector(
                username="your_username",
                password="your_password",
                security_token="your_token"
            )
            
            # Using AWS Secrets Manager
            connector = ScalableSalesforceConnector(
                secret_name="your_secret_name",
                region_name="us-east-1"
            )
        """
        self.logger = setup_logging()
        
        # Initialize credentials
        sf_username = username
        sf_password = password
        sf_token = security_token
        
        # If secret_name is provided, get credentials from Secrets Manager
        if secret_name:
            self.logger.info(f"Retrieving credentials from Secrets Manager: {secret_name}")
            try:
                secrets_manager = SecretsManager(region_name)
                credentials = secrets_manager.get_secret(secret_name)
                sf_username = credentials.get('salesforce_username')
                sf_password = credentials.get('salesforce_password')
                sf_token = credentials.get('salesforce_security_token')
            except Exception as e:
                self.logger.error(f"Failed to retrieve credentials from Secrets Manager: {str(e)}")
                raise
        
        # Validate that we have all required credentials
        if not all([sf_username, sf_password, sf_token]):
            raise ValueError(
                "Must provide either all credentials (username, password, security_token) "
                "or a valid secret_name for AWS Secrets Manager"
            )

        # Initialize Spark
        if glue_context:
            self.spark = get_glue_spark_session(glue_context, spark_config)
        else:
            self.spark = initialize_spark(spark_config)
        
        self.cache = FieldCache(cache_size)
        
        # Initialize Salesforce connection
        try:
            self.sf = Salesforce(
                username=sf_username,
                password=sf_password,
                security_token=sf_token,
                domain=domain,
                version=version
            )
            self.logger.info("Successfully connected to Salesforce")
        except Exception as e:
            self.logger.error(f"Failed to connect to Salesforce: {str(e)}")
            raise

        self.extractor = DataExtractor(self.sf, self.spark, self.cache, self.logger)
        self.loader = DataLoader(self.sf, self.logger)

    def extract_data(self, query: str, partition_field: str = None, 
                    num_partitions: int = 10):
        return self.extractor.extract_data(query, partition_field, num_partitions)

    def load_data(self, object_name: str, data, operation: str = 'insert', 
                 batch_size: int = 10000):
        return self.loader.load_data(object_name, data, operation, batch_size)

    def clear_caches(self):
        self.cache.clear() 