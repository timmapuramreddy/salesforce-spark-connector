from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime

@dataclass
class BatchResult:
    """Class to store batch processing results"""
    success: bool
    record_id: str = None
    error_message: str = None
    original_record: Dict = None
    created_date: str = None

class DataLoader:
    def __init__(self, sf, logger):
        self.sf = sf
        self.logger = logger

    def load_data(self, object_name: str, data, operation: str = 'insert', 
                 batch_size: int = 10000):
        """Load data into Salesforce with detailed error tracking."""
        try:
            record_count = len(data) if isinstance(data, list) else data.count()
            self.logger.info(f"Starting {operation} operation with {record_count} records")
            
            # Convert DataFrame to list of dictionaries if needed
            records = data if isinstance(data, list) else [row.asDict() for row in data.collect()]
            
            success_records = []
            failed_records = []
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                batch_results = self._process_batch(object_name, batch, operation)
                
                # Process results for each record in the batch
                for idx, result in enumerate(batch_results):
                    original_record = batch[idx]
                    if result.get('success'):
                        success_records.append(
                            BatchResult(
                                success=True,
                                record_id=result.get('id'),
                                original_record=original_record,
                                created_date=datetime.now().isoformat()
                            )
                        )
                    else:
                        error_message = '; '.join([err.get('message', 'Unknown error') 
                                                 for err in result.get('errors', [])])
                        failed_records.append(
                            BatchResult(
                                success=False,
                                error_message=error_message,
                                original_record=original_record,
                                created_date=datetime.now().isoformat()
                            )
                        )
                
                # Log batch statistics
                self.logger.info(
                    f"Batch processed: {len(batch)} records. "
                    f"Success: {len([r for r in batch_results if r.get('success')])}. "
                    f"Failed: {len([r for r in batch_results if not r.get('success')])}"
                )
            
            # Prepare final response
            final_response = {
                'total_records': record_count,
                'successful_records': len(success_records),
                'failed_records': len(failed_records),
                'success_details': success_records,
                'failure_details': failed_records
            }
            
            # Log final statistics
            self.logger.info(
                f"Operation completed. Total: {record_count}, "
                f"Successful: {len(success_records)}, "
                f"Failed: {len(failed_records)}"
            )
            
            return final_response
            
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise

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

    def retry_failed_records(self, object_name: str, failed_records: List[BatchResult], 
                           operation: str = 'insert', batch_size: int = 10000):
        """Retry failed records."""
        records_to_retry = [record.original_record for record in failed_records]
        return self.load_data(object_name, records_to_retry, operation, batch_size)