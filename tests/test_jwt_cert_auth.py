# tests/test_jwt_cert_auth.py
# JWT Certificate Authentication:

from salesforce_connector import ScalableSalesforceConnector
from salesforce_connector.config import SalesforceConfig, ProcessingConfig

def test_jwt_cert_auth():
    sf_config = SalesforceConfig(
        auth_method='jwt_cert',
        client_id='your_client_id',
        username='your_username',
        private_key_path='path/to/your/private.key'  # or use private_key with the key content
    )
    
    proc_config = ProcessingConfig(require_spark=False)

    try:
        connector = ScalableSalesforceConnector(
            sf_config=sf_config,
            processing_config=proc_config
        )

        # Test connection
        results = connector.extract_data("SELECT Id FROM Account LIMIT 5")
        print("JWT Cert Auth Success:", results)

    except Exception as e:
        print(f"JWT Cert Auth Failed: {str(e)}") 