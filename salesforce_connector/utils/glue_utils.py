from typing import Dict, Optional
from pyspark.sql import SparkSession

def get_glue_spark_session(
    glue_context,
    additional_config: Optional[Dict[str, str]] = None
) -> SparkSession:
    """
    Get or create a Spark session optimized for AWS Glue.
    
    Args:
        glue_context: The AWS Glue context
        additional_config: Additional Spark configuration parameters
        
    Returns:
        SparkSession: Configured Spark session
    """
    default_config = {
        "spark.sql.broadcastTimeout": "3600",           # Timeout for broadcast joins (in seconds)
        "spark.sql.shuffle.partitions": "200",          # Number of partitions for shuffling
        "spark.default.parallelism": "100",             # Default number of parallel tasks
        "spark.dynamicAllocation.enabled": "true",      # Enable dynamic allocation of executors
        "spark.dynamicAllocation.initialExecutors": "2", # Initial number of executors
        "spark.dynamicAllocation.minExecutors": "2",    # Minimum number of executors
        "spark.dynamicAllocation.maxExecutors": "10"    # Maximum number of executors
    }
    
    if additional_config:
        default_config.update(additional_config)
    
    spark = glue_context.spark_session
    
    for key, value in default_config.items():
        spark.conf.set(key, value)
    
    return spark 