from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass
class SalesforceConfig:
    """Configuration for Salesforce connection."""
    auth_method: str = 'oauth'
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    security_token: Optional[str] = None
    private_key: Optional[str] = None
    private_key_path: Optional[str] = None
    redirect_uri: Optional[str] = None
    auth_url: Optional[str] = None
    token_url: Optional[str] = None
    domain: str = 'login'
    version: str = '54.0'

@dataclass
class AWSConfig:
    """Configuration for AWS services."""
    secret_name: Optional[str] = None
    region_name: Optional[str] = None
    secret_values: Optional[Dict] = None

@dataclass
class ProcessingConfig:
    """Configuration for data processing engines."""
    spark_config: Optional[Dict[str, str]] = None
    cache_size: int = 100
    glue_context: Any = None
    require_spark: bool = False
    require_duckdb: bool = False
    duckdb_memory_limit: str = '2GB' 