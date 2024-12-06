import re
from typing import List, Tuple, Any
from datetime import datetime

def get_object_name(query: str) -> str:
    """Extract object name from SOQL query."""
    match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
    if match:
        return match.group(1)
    raise ValueError("Could not extract object name from query")

def create_partition_ranges(field_type: str, min_val: Any, max_val: Any, 
                          num_partitions: int) -> List[Tuple]:
    """Create partition ranges based on field type."""
    ranges = []
    
    if field_type == 'datetime':
        min_date = datetime.strptime(min_val, '%Y-%m-%dT%H:%M:%S.%fZ')
        max_date = datetime.strptime(max_val, '%Y-%m-%dT%H:%M:%S.%fZ')
        delta = (max_date - min_date) / num_partitions
        
        for i in range(num_partitions):
            start_date = min_date + (delta * i)
            end_date = min_date + (delta * (i + 1))
            ranges.append((
                start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            ))
            
    elif field_type == 'number':
        min_num = float(min_val)
        max_num = float(max_val)
        step = (max_num - min_num) / num_partitions
        
        for i in range(num_partitions):
            start_num = min_num + (step * i)
            end_num = min_num + (step * (i + 1))
            ranges.append((start_num, end_num))
            
    else:
        # For string fields, create alphabetical ranges
        alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        step = max(1, len(alphabet) // num_partitions)
        
        for i in range(0, len(alphabet), step):
            start_char = alphabet[i]
            end_char = alphabet[min(i + step, len(alphabet) - 1)]
            ranges.append((start_char, end_char))
    
    return ranges 