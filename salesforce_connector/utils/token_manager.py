from typing import Dict, Optional
import time
from datetime import datetime, timedelta
from requests_oauthlib import OAuth2Session

class InMemoryTokenStore:
    """Thread-safe in-memory token storage."""
    _instance = None
    _token = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(InMemoryTokenStore, cls).__new__(cls)
        return cls._instance

    @classmethod
    def get_token(cls) -> Optional[Dict]:
        return cls._token

    @classmethod
    def set_token(cls, token: Dict) -> None:
        if token:
            token['stored_at'] = datetime.now().isoformat()
        cls._token = token

class TokenManager:
    def __init__(self, 
                 client_id: str,
                 client_secret: str,
                 token_url: str):
        """
        Initialize TokenManager for handling OAuth2 tokens.
        
        Args:
            client_id: OAuth2 client ID
            client_secret: OAuth2 client secret
            token_url: Token endpoint URL
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.token_store = InMemoryTokenStore()

    def get_token(self) -> Optional[Dict]:
        """Get current token."""
        return self.token_store.get_token()

    def set_token(self, token: Dict) -> None:
        """Set token in memory."""
        self.token_store.set_token(token)

    def is_token_expired(self, token: Dict) -> bool:
        """Check if token is expired or about to expire."""
        if not token:
            return True

        # Add buffer time (e.g., 5 minutes) to ensure we refresh before expiration
        buffer_time = 300  # 5 minutes in seconds
        
        # Check if token has expires_at
        if 'expires_at' in token:
            return token['expires_at'] - buffer_time < time.time()
        
        # If no expires_at, check expires_in
        if 'expires_in' in token:
            stored_at = datetime.fromisoformat(token.get('stored_at', datetime.now().isoformat()))
            expiration_time = stored_at + timedelta(seconds=token['expires_in'])
            return expiration_time - timedelta(seconds=buffer_time) < datetime.now()
            
        return False

    def refresh_token(self, token: Dict) -> Dict:
        """Refresh the OAuth2 token."""
        extra = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
        }

        oauth = OAuth2Session(self.client_id, token=token)
        
        try:
            new_token = oauth.refresh_token(
                self.token_url,
                refresh_token=token['refresh_token'],
                **extra
            )
            self.set_token(new_token)
            return new_token
        except Exception as e:
            self.set_token(None)  # Clear invalid token
            raise Exception(f"Failed to refresh token: {str(e)}")

    def get_valid_token(self) -> Dict:
        """Get a valid token, refreshing if necessary."""
        token = self.get_token()
        
        if not token:
            raise Exception("No token available. Please authenticate first.")
            
        if self.is_token_expired(token):
            token = self.refresh_token(token)
            
        return token 