import pytest
from salesforce_connector import ScalableSalesforceConnector

def test_connector_initialization():
    """Test that connector initialization fails with invalid credentials"""
    with pytest.raises(Exception):
        ScalableSalesforceConnector(
            username="fake_username",
            password="fake_password",
            security_token="fake_token"
        )

# Add more tests as needed 