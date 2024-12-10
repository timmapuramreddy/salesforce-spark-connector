# Salesforce Spark Connector

A scalable connector for Salesforce data operations using Apache Spark, specifically designed for AWS Glue integration.

## Features

- Parallel data extraction from Salesforce
- Field type caching for better performance
- Bulk data loading into Salesforce
- AWS Glue integration
- AWS Secrets Manager support for secure credential management
- Spark optimization for large-scale data processing

## Installation

```bash
pip install salesforce-spark-connector
```

## Usage

### Using Direct Credentials

```python
from salesforce_connector import ScalableSalesforceConnector

# Initialize with direct credentials
connector = ScalableSalesforceConnector(
    username="your_username",
    password="your_password",
    security_token="your_token"
)

# Extract data
query = "SELECT Id, Name FROM Account"
df = connector.extract_data(query)

# Load data
data_to_load = spark.createDataFrame([...])  # Your data
result = connector.load_data(
    object_name="Account",
    data=data_to_load,
    operation="insert"  # Supports "insert", "update", "upsert"
)
```

### Using AWS Secrets Manager

```python
# Initialize with AWS Secrets Manager
connector = ScalableSalesforceConnector(
    secret_name="your_secret_name",
    region_name="us-east-1"
)
```

### AWS Secrets Manager Format
The secret in AWS Secrets Manager should be stored in the following JSON format:

```json
{
    "salesforce_username": "your-username",
    "salesforce_password": "your-password",
    "salesforce_security_token": "your-security-token"
}
```

### AWS Glue Integration

```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Initialize Glue context
glueContext = GlueContext(SparkContext.getOrCreate())

# Initialize connector with AWS Secrets Manager
connector = ScalableSalesforceConnector(
    secret_name="your_secret_name",
    region_name="us-east-1",
    glue_context=glueContext
)

# Extract data with partitioning for better performance
query = "SELECT Id, Name FROM Account"
df = connector.extract_data(
    query=query,
    partition_field="CreatedDate",
    num_partitions=10
)

# Convert to Glue DynamicFrame and process further
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "transformed_data")
```

## Configuration

### Required IAM Permissions for AWS Secrets Manager

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue"
            ],
            "Resource": [
                "arn:aws:secretsmanager:region:account-id:secret:secret-name-*"
            ]
        }
    ]
}
```

## Development

```bash
# Clone the repository
git clone https://github.com/timmapuramreddy/salesforce-spark-connector.git
cd salesforce-spark-connector

# Install development dependencies
pip install -e .[dev]

# Run tests
pytest

# Run linting
flake8
```

## Error Handling

The connector provides detailed error handling for both extraction and loading operations. Failed records during bulk loading are tracked and can be retried:

```python
# Load data with error handling
result = connector.load_data(object_name="Account", data=df)
if result['failed_records'] > 0:
    print(f"Failed records: {result['failure_details']}")
    # Retry failed records
    retry_result = connector.loader.retry_failed_records(
        object_name="Account",
        failed_records=result['failure_details']
    )
```

## License

MIT License
