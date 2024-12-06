from typing import Dict
from pyspark.sql import SparkSession

def initialize_spark(spark_config: Dict[str, str] = None) -> SparkSession:
    """Initialize Spark session with optimal configurations."""
    builder = SparkSession.builder.appName("SalesforceConnector")
    
    default_config = {
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",
        "spark.sql.shuffle.partitions": "200",
        "spark.default.parallelism": "100",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": "2g"
    }
    
    if spark_config:
        default_config.update(spark_config)
    
    for key, value in default_config.items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate() 