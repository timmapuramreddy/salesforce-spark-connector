# tests/test_oauth_pkce_auth.py
# OAuth2 PKCE Authentication:

from salesforce_connector import ScalableSalesforceConnector
from salesforce_connector.config import SalesforceConfig, ProcessingConfig

def test_oauth_pkce_auth():
    sf_config = SalesforceConfig(
        auth_method='oauth',
        client_id='your_client_id',
        client_secret='your_client_secret',
        redirect_uri='your_redirect_uri',
        auth_url='https://login.salesforce.com/services/oauth2/authorize',
        token_url='https://login.salesforce.com/services/oauth2/token'
    )
    
    proc_config = ProcessingConfig(require_spark=False)

    try:
        connector = ScalableSalesforceConnector(
            sf_config=sf_config,
            processing_config=proc_config
        )

        # Test connection
        results = connector.extract_data("SELECT Id FROM Account LIMIT 5")
        print("OAuth2 PKCE Auth Success:", results)

    except Exception as e:
        print(f"OAuth2 PKCE Auth Failed: {str(e)}") 