from typing import Dict, Optional
from pyspark.sql import SparkSession
import os
import sys

def initialize_spark(spark_config: Dict[str, str] = None, fallback_to_basic: bool = True) -> Optional[SparkSession]:
    """Initialize Spark session with optimal configurations."""
    # Default configurations
    default_config = {
        "spark.app.name": "Salesforce Connector",
        # Java 11 specific configurations
        "spark.driver.extraJavaOptions": (
            "-Djava.security.manager=allow "
            "-Dio.netty.tryReflectionSetAccessible=true"
        ),
        # Local mode configurations
        "spark.master": "local[*]",
        "spark.driver.host": "localhost",
        # Memory configurations
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g",
        # Disable Hive support for local testing
        "spark.sql.catalogImplementation": "in-memory",
        # Additional configurations for stability
        "spark.driver.extraClassPath": os.environ.get("SPARK_CLASSPATH", ""),
        "spark.executor.extraClassPath": os.environ.get("SPARK_CLASSPATH", ""),
    }

    try:
        # Create builder with default configurations
        builder = SparkSession.builder

        # Apply default configurations
        for key, value in default_config.items():
            builder = builder.config(key, value)

        # Apply custom configurations if provided
        if spark_config:
            for key, value in spark_config.items():
                builder = builder.config(key, value)

        # Try to create the SparkSession
        spark = builder.getOrCreate()
        
        # Test the session
        spark.sql("SELECT 1").collect()
        return spark

    except Exception as e:
        if fallback_to_basic:
            try:
                # Try with minimal configuration
                minimal_config = {
                    "spark.app.name": "Salesforce Connector Basic",
                    "spark.master": "local[*]",
                    "spark.driver.extraJavaOptions": "-Djava.security.manager=allow",
                }
                
                # Stop any existing session
                SparkSession.builder.getOrCreate().stop()
                
                # Create new session with minimal config
                spark = SparkSession.builder
                for key, value in minimal_config.items():
                    spark = spark.config(key, value)
                return spark.getOrCreate()
            except Exception as e2:
                print(f"Failed to create even basic Spark session: {str(e2)}")
                if not fallback_to_basic:
                    raise
                return None
        else:
            raise