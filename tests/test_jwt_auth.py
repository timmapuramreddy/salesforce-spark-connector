# tests/test_jwt_auth.py
# JWT Secret Authentication:

from salesforce_connector import ScalableSalesforceConnector
from salesforce_connector.config import SalesforceConfig, AWSConfig, ProcessingConfig

def test_jwt_secret_auth():
    # Configuration
    sf_config = SalesforceConfig(
        auth_method='jwt_secret',
        version='54.0'
    )
    
    aws_config = AWSConfig(
        secret_values={
            'host': 'https://login.salesforce.com',  # or test.salesforce.com for sandbox
            'auth_token': 'your_jwt_token_here'
        }
    )
    
    proc_config = ProcessingConfig(
        require_spark=False,
        duckdb_memory_limit='2GB'
    )

    try:
        # Initialize connector
        connector = ScalableSalesforceConnector(
            sf_config=sf_config,
            aws_config=aws_config,
            processing_config=proc_config
        )

        # Test connection
        results = connector.extract_data("SELECT Id FROM Account LIMIT 5")
        print("JWT Secret Auth Success:", results)
        
        # Test DuckDB query
        analysis = connector.query_data("""
            SELECT COUNT(*) as count 
            FROM sf_data_latest
        """)
        print("DuckDB Query Result:", analysis)

    except Exception as e:
        print(f"JWT Secret Auth Failed: {str(e)}") 