import json
import boto3
from typing import Dict, Optional
from botocore.exceptions import ClientError

class SecretsManager:
    def __init__(self, region_name: Optional[str] = None):
        self.session = boto3.session.Session()
        self.client = self.session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

    def get_secret(self, secret_name: str) -> Dict:
        """
        Retrieve a secret from AWS Secrets Manager.
        
        Args:
            secret_name: The name or ARN of the secret
            
        Returns:
            Dict containing the secret key/value pairs
            
        Raises:
            ClientError: If there's an error retrieving the secret
        """
        try:
            response = self.client.get_secret_value(SecretId=secret_name)
            if 'SecretString' in response:
                return json.loads(response['SecretString'])
            raise ValueError("Secret value is not a string")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                raise Exception("Error: Unable to decrypt secret using provided KMS key")
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                raise Exception("Error: Internal service error in Secrets Manager")
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                raise Exception("Error: Invalid parameter in request")
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                raise Exception("Error: Invalid request to Secrets Manager")
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise Exception(f"Error: Secret {secret_name} not found")
            else:
                raise Exception(f"Error retrieving secret: {str(e)}") 