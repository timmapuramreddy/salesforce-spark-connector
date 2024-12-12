# Salesforce Spark Connector

A scalable Python connector for Salesforce that supports multiple data processing engines (Apache Spark, DuckDB) and various authentication methods.

## Features

- **Multiple Authentication Methods**:
  - OAuth2 with PKCE
  - JWT Bearer Flow
  - Certificate-based JWT
  - Username/Password
  - AWS Secrets Manager integration

- **Flexible Data Processing**:
  - Apache Spark support
  - DuckDB integration for efficient local processing
  - AWS Glue compatibility
  - Automatic format detection and conversion

 **Advanced Extraction Capabilities**
  - Parallel data extraction with automatic partitioning
  - Smart query partitioning based on field types
  - Automatic format conversion
  - Configurable number of partitions

- **Performance Optimizations**:
  - Efficient data pagination
  - Field type caching
  - Memory-efficient processing
  - Configurable batch sizes

- **Data Formats**:
  - Spark DataFrames
  - DuckDB Relations
  - AWS Glue DynamicFrames
  - Pandas DataFrames
  - Dictionary/JSON

## Installation

Basic installation:
```bash
pip install salesforce-spark-connector
```

With Spark support:
```bash
pip install "salesforce-spark-connector[spark]"
```

With development tools:
```bash
pip install "salesforce-spark-connector[dev]"
```

## Quick Start

```python
from salesforce_connector import ScalableSalesforceConnector
from salesforce_connector.config import SalesforceConfig, AWSConfig, ProcessingConfig

# Configure the connector
sf_config = SalesforceConfig(
    auth_method='jwt_secret',
    client_id='your_client_id',
    username='your_username'
)

aws_config = AWSConfig(
    secret_values={
        'host': 'https://login.salesforce.com',
        'auth_token': 'your_jwt_token'
    }
)

# Initialize connector
connector = ScalableSalesforceConnector(
    sf_config=sf_config,
    aws_config=aws_config
)

# Extract data with automatic format detection
data = connector.extract_data("SELECT Id, Name FROM Account LIMIT 5")

# Use DuckDB for analysis
result = connector.query_data("""
    SELECT Industry, COUNT(*) as count
    FROM sf_data_latest
    GROUP BY Industry
    ORDER BY count DESC
""")
```

## Authentication Methods

### JWT Bearer Flow
```python
sf_config = SalesforceConfig(
    auth_method='jwt_secret',
    client_id='your_client_id'
)
aws_config = AWSConfig(
    secret_values={'auth_token': 'your_jwt_token'}
)
```

### OAuth2 with PKCE
```python
sf_config = SalesforceConfig(
    auth_method='oauth',
    client_id='your_client_id',
    client_secret='your_client_secret',
    redirect_uri='your_redirect_uri'
)
```

### Username/Password
```python
sf_config = SalesforceConfig(
    auth_method='password',
    username='your_username',
    password='your_password',
    security_token='your_security_token'
)
```

## Data Processing Examples

### Using DuckDB
```python
# Extract and analyze data
data = connector.extract_data("SELECT Id, Amount FROM Opportunity")
analysis = connector.query_data("""
    SELECT 
        DATE_TRUNC('month', CloseDate) as month,
        SUM(Amount) as total_amount
    FROM sf_data_latest
    GROUP BY 1
    ORDER BY 1
""")
```

### Using Spark
```python
proc_config = ProcessingConfig(require_spark=True)
connector = ScalableSalesforceConnector(
    sf_config=sf_config,
    processing_config=proc_config
)

# Get Spark DataFrame
spark_df = connector.extract_data(
    "SELECT Id, Name FROM Account",
    output_format='spark'
)

# Extract data with parallel processing
results = connector.extract_data(
    "SELECT Id, Name, CreatedDate FROM Account",
    partition_field="CreatedDate",
    num_partitions=5,
    output_format='spark'
)

```

### Using AWS Glue
```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glue_context = GlueContext(sc)

proc_config = ProcessingConfig(glue_context=glue_context)
connector = ScalableSalesforceConnector(
    sf_config=sf_config,
    processing_config=proc_config
)

# Get DynamicFrame
dynamic_frame = connector.extract_data(
    "SELECT Id, Name FROM Account",
    output_format='dynamicframe'
)
```

## Configuration Options

### SalesforceConfig
- `auth_method`: Authentication method ('oauth', 'jwt', 'password')
- `client_id`: OAuth client ID
- `client_secret`: OAuth client secret (optional)
- `username`: Salesforce username (for password auth)
- `password`: Salesforce password (for password auth)
- `security_token`: Security token (for password auth)
- `domain`: Salesforce domain (default: 'login')
- `version`: API version (default: '54.0')

### ProcessingConfig
- `require_spark`: Enable Spark processing
- `require_duckdb`: Enable DuckDB support
- `spark_config`: Spark configuration options
- `duckdb_memory_limit`: DuckDB memory limit
- `cache_size`: Field metadata cache size

## Dependencies

- `simple-salesforce>=1.12.4`
- `requests-oauthlib>=1.3.1`
- `pyspark>=3.0.0` (optional)
- `duckdb>=0.9.0` (optional)
- `pandas>=1.3.0`
- `pyarrow>=14.0.1`

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
