from typing import Dict, Optional

def get_field_type(field_info: Dict) -> str:
    """
    Map Salesforce field types to simplified types.
    
    Returns:
        str: One of 'datetime', 'number', 'id', or 'string'
    """
    type_mapping = {
        'datetime': 'datetime',
        'date': 'datetime',
        'time': 'datetime',
        'int': 'number',
        'double': 'number',
        'currency': 'number',
        'percent': 'number',
        'decimal': 'number',
        'id': 'id',
        'reference': 'id'
    }
    
    salesforce_type = field_info['type']
    return type_mapping.get(salesforce_type, 'string') 