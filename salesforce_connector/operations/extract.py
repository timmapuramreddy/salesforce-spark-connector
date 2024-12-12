from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional, Union
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from ..types.field_types import get_field_type
from ..utils.query_utils import get_object_name, create_partition_ranges, get_sobject

class DataExtractor:
    def __init__(self, sf, spark, cache, logger):
        self.sf = sf
        self.spark = spark
        self.cache = cache
        self.logger = logger

    def extract_data(self, query: str, partition_field: Optional[str] = None, 
                    num_partitions: int = 10) -> Union[List[Dict[str, Any]], DataFrame]:
        """
        Extract data from Salesforce with automatic handling of parallel extraction.
        
        Args:
            query: SOQL query string
            partition_field: Field to use for partitioning (optional)
            num_partitions: Number of partitions for parallel extraction
            
        Returns:
            Either a list of dictionaries or a Spark DataFrame depending on environment
        """
        try:
            # Use parallel extraction if partition field is provided and Spark is available
            if partition_field and self.spark:
                self.logger.info(f"Using parallel extraction with {num_partitions} partitions")
                return self._extract_parallel(query, partition_field, num_partitions)
            else:
                self.logger.info("Using simple extraction")
                return self._extract_single(query)
        except Exception as e:
            self.logger.error(f"Error extracting data: {str(e)}")
            raise

    def _extract_parallel(self, base_query: str, partition_field: str, 
                         num_partitions: int) -> DataFrame:
        """Extract data in parallel using partitioning."""
        object_name = get_object_name(base_query)
        sobject = get_sobject(self.sf, object_name)
        object_describe = sobject.describe()
        
        try:
            # Get field description from Salesforce
            field_info = next(
                (field for field in object_describe['fields'] 
                 if field['name'].lower() == partition_field.lower()),
                None
            )
            
            if not field_info:
                raise ValueError(f"Field {partition_field} not found in {object_name}")
            
            # Get min and max values
            bounds_query = f"SELECT MIN({partition_field}), MAX({partition_field}) FROM {object_name}"
            bounds = self.sf.query(bounds_query)['records'][0]
            min_val = bounds[f'expr0']
            max_val = bounds[f'expr1']
            
            if not min_val or not max_val:
                self.logger.warning(f"No data range found for {partition_field}, falling back to single extraction")
                return self._extract_single(base_query)
            
            # Get field type and create ranges
            field_type = get_field_type(field_info)
            ranges = create_partition_ranges(field_type, min_val, max_val, num_partitions)
            
            # Extract in parallel
            with ThreadPoolExecutor(max_workers=num_partitions) as executor:
                futures = []
                for range_start, range_end in ranges:
                    partitioned_query = self._create_partitioned_query(
                        base_query, partition_field, range_start, range_end, field_type
                    )
                    futures.append(executor.submit(self._extract_single, partitioned_query))
                
                results = []
                for future in futures:
                    if isinstance(future.result(), DataFrame):
                        results.extend(future.result().collect())
                    else:
                        results.extend(future.result())
            
            # Convert to Spark DataFrame
            return self.spark.createDataFrame(results)
            
        except Exception as e:
            self.logger.error(f"Parallel extraction failed: {str(e)}")
            self.logger.info("Falling back to single extraction")
            return self._extract_single(base_query)

    def _create_partitioned_query(self, base_query: str, partition_field: str,
                                range_start: Any, range_end: Any, 
                                field_type: str) -> str:
        """Create a query for a specific partition range."""
        # Check if query already has WHERE clause
        if "WHERE" in base_query.upper():
            connector = "AND"
        else:
            connector = "WHERE"

        if field_type == 'datetime':
            return f"{base_query} {connector} {partition_field} >= {range_start} AND {partition_field} < {range_end}"
        elif field_type == 'number':
            return f"{base_query} {connector} {partition_field} >= {range_start} AND {partition_field} < {range_end}"
        else:
            # For string/id fields
            return f"{base_query} {connector} {partition_field} >= '{range_start}' AND {partition_field} < '{range_end}'"

    def _extract_single(self, query: str) -> Union[List[Dict[str, Any]], DataFrame]:
        """Extract data using a single query."""
        results = self.sf.query_all(query)
        
        if not results['records']:
            return [] if not self.spark else self.spark.createDataFrame([], self._infer_schema(query))
        
        clean_records = [
            {k:v for k,v in record.items() if k != 'attributes'}
            for record in results['records']
        ]
        
        return clean_records if not self.spark else self.spark.createDataFrame(clean_records)

    def _infer_schema(self, query: str) -> StructType:
        """Infer schema from query fields."""
        return StructType([
            StructField("Id", StringType(), True),
            StructField("Name", StringType(), True)
        ])

    # ... rest of extraction methods ... 