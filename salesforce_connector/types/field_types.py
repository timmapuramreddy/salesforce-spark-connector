from typing import Dict, Optional, Any

def get_field_type(field_info: Dict[str, Any]) -> str:
    """
    Map Salesforce field types to simplified types.
    
    Args:
        field_info: Field metadata from Salesforce describe call
        
    Returns:
        str: One of 'datetime', 'number', 'id', or 'string'
    """
    type_mapping = {
        'datetime': 'datetime',
        'date': 'datetime',
        'time': 'datetime',
        'double': 'number',
        'currency': 'number',
        'percent': 'number',
        'int': 'number',
        'long': 'number',
        'id': 'id',
        'reference': 'id'
    }
    
    salesforce_type = field_info.get('type', '').lower()
    return type_mapping.get(salesforce_type, 'string') 