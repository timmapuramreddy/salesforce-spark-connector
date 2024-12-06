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
        "spark.sql.broadcastTimeout": "3600",
        "spark.sql.shuffle.partitions": "200",
        "spark.default.parallelism": "100",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.initialExecutors": "2",
        "spark.dynamicAllocation.minExecutors": "2",
        "spark.dynamicAllocation.maxExecutors": "10"
    }
    
    if additional_config:
        default_config.update(additional_config)
    
    spark = glue_context.spark_session
    
    for key, value in default_config.items():
        spark.conf.set(key, value)
    
    return spark 