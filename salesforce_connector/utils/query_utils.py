import re
from typing import List, Tuple, Any, Union
from datetime import datetime

def get_object_name(query: str) -> str:
    """Extract object name from SOQL query."""
    match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
    if not match:
        raise ValueError("Could not determine object name from query")
    return match.group(1)

def get_sobject(sf, object_name: str):
    """Get SObject from Salesforce connection."""
    return getattr(sf, object_name)

def parse_salesforce_datetime(dt_str: str) -> datetime:
    """Parse Salesforce datetime string to Python datetime."""
    # Remove the timezone offset and 'Z' if present
    dt_str = re.sub(r'[+-]\d{4}$', 'Z', dt_str)
    try:
        return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError:
        # Try without milliseconds
        return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S%z')

def create_partition_ranges(field_type: str, min_val: Any, max_val: Any, 
                          num_partitions: int) -> List[Tuple[str, str]]:
    """Create partition ranges based on field type."""
    if not min_val or not max_val:
        return []

    if field_type == 'datetime':
        try:
            min_date = parse_salesforce_datetime(min_val)
            max_date = parse_salesforce_datetime(max_val)
            
            # Calculate time delta for each partition
            delta = (max_date - min_date) / num_partitions
            
            ranges = []
            for i in range(num_partitions):
                start_date = min_date + (delta * i)
                end_date = min_date + (delta * (i + 1))
                
                # Format dates back to Salesforce format
                start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                
                ranges.append((start_str, end_str))
            return ranges
            
        except Exception as e:
            raise ValueError(f"Failed to create datetime ranges: {str(e)}")
            
    elif field_type == 'number':
        try:
            min_num = float(min_val)
            max_num = float(max_val)
            step = (max_num - min_num) / num_partitions
            return [(str(min_num + (step * i)), str(min_num + (step * (i + 1))))
                   for i in range(num_partitions)]
        except Exception as e:
            raise ValueError(f"Failed to create numeric ranges: {str(e)}")
            
    else:  # string or id type
        # For string/id fields, use alphabetical partitioning
        try:
            chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
            step = len(chars) // num_partitions
            ranges = []
            
            for i in range(0, len(chars), step):
                start_char = chars[i]
                end_char = chars[min(i + step, len(chars) - 1)]
                ranges.append((start_char, end_char))
                
            return ranges
        except Exception as e:
            raise ValueError(f"Failed to create string ranges: {str(e)}")
    
    return ranges 