from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from ..types.field_types import get_field_type
from ..utils.query_utils import get_object_name, create_partition_ranges

class DataExtractor:
    def __init__(self, sf, spark, cache, logger):
        self.sf = sf
        self.spark = spark
        self.cache = cache
        self.logger = logger

    def extract_data(self, query: str, partition_field: str = None, 
                    num_partitions: int = 10) -> DataFrame:
        try:
            if partition_field:
                return self._extract_parallel(query, partition_field, num_partitions)
            else:
                return self._extract_single(query)
        except Exception as e:
            self.logger.error(f"Error extracting data: {str(e)}")
            raise

    def _extract_parallel(self, base_query: str, partition_field: str, 
                         num_partitions: int) -> DataFrame:
        """Extract data in parallel using partitioning."""
        object_name = get_object_name(base_query)
        
        # Get min and max values
        bounds_query = f"SELECT MIN({partition_field}), MAX({partition_field}) FROM {object_name}"
        bounds = self.sf.query(bounds_query)['records'][0]
        min_val = bounds[f'expr0']
        max_val = bounds[f'expr1']
        
        # Get field type and create ranges
        field_info = self.cache.get_object_describe(self.sf, object_name)
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
            
            dfs = [future.result() for future in futures]
            
        return self.spark.createDataFrame(
            self.spark.sparkContext.union([df.rdd for df in dfs])
        )

    def _create_partitioned_query(self, base_query: str, partition_field: str,
                                range_start: Any, range_end: Any, 
                                field_type: str) -> str:
        """Create a query for a specific partition range."""
        if field_type == 'datetime':
            return f"{base_query} AND {partition_field} >= {range_start} AND {partition_field} < {range_end}"
        elif field_type == 'number':
            return f"{base_query} AND {partition_field} >= {range_start} AND {partition_field} < {range_end}"
        else:
            return f"{base_query} AND {partition_field} >= '{range_start}' AND {partition_field} < '{range_end}'"

    def _extract_single(self, query: str) -> DataFrame:
        """Extract data using a single query."""
        results = self.sf.query_all(query)
        
        if not results['records']:
            return self.spark.createDataFrame([], self._infer_schema(query))
        
        clean_records = [
            {k:v for k,v in record.items() if k != 'attributes'}
            for record in results['records']
        ]
        
        return self.spark.createDataFrame(clean_records)

    def _infer_schema(self, query: str) -> StructType:
        """Infer schema from query fields."""
        return StructType([
            StructField("Id", StringType(), True),
            StructField("Name", StringType(), True)
        ])

    # ... rest of extraction methods ... 