# tests/test_user_password_auth.py
# User and Password Authentication:

from salesforce_connector import ScalableSalesforceConnector
from salesforce_connector.config import SalesforceConfig, ProcessingConfig

def test_password_auth():
    sf_config = SalesforceConfig(
        auth_method='password',
        username='your_username',
        password='your_password',
        security_token='your_security_token',
        domain='login'  # or 'test' for sandbox
    )
    
    proc_config = ProcessingConfig(require_spark=False)

    try:
        connector = ScalableSalesforceConnector(
            sf_config=sf_config,
            processing_config=proc_config
        )

        # Test connection
        results = connector.extract_data("SELECT Id FROM Account LIMIT 5")
        print("Password Auth Success:", results)

    except Exception as e:
        print(f"Password Auth Failed: {str(e)}")