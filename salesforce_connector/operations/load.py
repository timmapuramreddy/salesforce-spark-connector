from typing import List, Dict
from pyspark.sql import DataFrame

class DataLoader:
    def __init__(self, sf, logger):
        self.sf = sf
        self.logger = logger

    def load_data(self, object_name: str, data: DataFrame, 
                 operation: str = 'insert', batch_size: int = 10000) -> List[Dict]:
        """Load data into Salesforce."""
        try:
            self.logger.info(f"Starting {operation} operation with {data.count()} records")
            
            results = []
            for batch in self._batch_iterator(data, batch_size):
                batch_results = self._process_batch(object_name, batch, operation)
                results.extend(batch_results)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise

    def _batch_iterator(self, df: DataFrame, batch_size: int):
        """Iterator for processing DataFrame in batches."""
        total_rows = df.count()
        for offset in range(0, total_rows, batch_size):
            yield df.limit(batch_size).offset(offset).collect()

    def _process_batch(self, object_name: str, batch: List[Dict], 
                      operation: str) -> List[Dict]:
        """Process a single batch of records."""
        if operation == 'insert':
            return self.sf.bulk.__getattr__(object_name).insert(batch)
        elif operation == 'update':
            return self.sf.bulk.__getattr__(object_name).update(batch)
        elif operation == 'upsert':
            return self.sf.bulk.__getattr__(object_name).upsert(batch, 'Id')
        else:
            raise ValueError(f"Unsupported operation: {operation}") 