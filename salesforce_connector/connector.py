from simple_salesforce import Salesforce
from typing import Dict, Optional
from .utils.logging_utils import setup_logging
from .utils.spark_utils import initialize_spark
from .cache.field_cache import FieldCache
from .operations.extract import DataExtractor
from .operations.load import DataLoader
from .utils.glue_utils import get_glue_spark_session

class ScalableSalesforceConnector:
    def __init__(self, username: str, password: str, security_token: str,
                 spark_config: Optional[Dict[str, str]] = None, domain: str = 'login',
                 version: str = '54.0', cache_size: int = 100, glue_context=None):
        self.logger = setup_logging()
        
        if glue_context:
            self.spark = get_glue_spark_session(glue_context, spark_config)
        else:
            self.spark = initialize_spark(spark_config)
        
        self.cache = FieldCache(cache_size)
        
        try:
            self.sf = Salesforce(
                username=username,
                password=password,
                security_token=security_token,
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