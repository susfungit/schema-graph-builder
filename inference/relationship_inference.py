"""
Relationship inference module - Infers foreign key relationships from database schemas
"""

import re
from typing import Dict, List, Any


def infer_relationships(schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Infer foreign key relationships from database schema.
    
    Args:
        schema: Database schema dictionary containing tables and columns
        
    Returns:
        Dictionary mapping table names to their relationship information:
        {
            'table_name': {
                'primary_key': str,
                'foreign_keys': [
                    {
                        'column': str,
                        'references': str,  # format: 'table.column'
                        'confidence': float
                    }
                ]
            }
        }
    """
    relationships = {}
    tables = schema.get('tables', [])
    
    # Build lookup tables
    table_primary_keys = {}
    for table in tables:
        table_name = table['name']
        pk = _find_primary_key(table)
        table_primary_keys[table_name] = pk
    
    # Infer relationships for each table
    for table in tables:
        table_name = table['name']
        relationships[table_name] = {
            'primary_key': table_primary_keys.get(table_name),
            'foreign_keys': []
        }
        
        # Look for foreign key columns
        for column in table['columns']:
            if column['primary_key']:
                continue  # Skip primary key columns
                
            column_name = column['name']
            
            # Try to match column to other tables' primary keys
            for ref_table_name, ref_pk in table_primary_keys.items():
                if ref_table_name == table_name:
                    continue  # Don't reference self
                    
                if ref_pk and _is_foreign_key_match(column_name, ref_pk, ref_table_name):
                    confidence = _calculate_confidence(column_name, ref_pk, ref_table_name)
                    
                    foreign_key = {
                        'column': column_name,
                        'references': f'{ref_table_name}.{ref_pk}',
                        'confidence': confidence
                    }
                    relationships[table_name]['foreign_keys'].append(foreign_key)
                    break  # Take the first (best) match
    
    return relationships


def _find_primary_key(table: Dict[str, Any]) -> str:
    """Find the primary key column for a table."""
    for column in table['columns']:
        if column.get('primary_key', False):
            return column['name']
    return None


def _is_foreign_key_match(column_name: str, ref_pk: str, ref_table_name: str) -> bool:
    """
    Determine if a column might be a foreign key to a reference table's primary key.
    
    Common patterns:
    - customer_id -> customers.customer_id
    - user_id -> users.user_id  
    - category_id -> categories.category_id
    - profile_id -> user_profiles.profile_id
    """
    column_lower = column_name.lower()
    ref_pk_lower = ref_pk.lower()
    ref_table_lower = ref_table_name.lower()
    
    # Exact match
    if column_lower == ref_pk_lower:
        return True
    
    # Column name matches primary key name
    if column_lower == ref_pk_lower:
        return True
        
    # Try to extract the base name from both
    column_base = _extract_base_name(column_lower)
    ref_base = _extract_base_name(ref_pk_lower)
    
    # Check if column base matches reference base
    if column_base and ref_base and column_base == ref_base:
        return True
    
    # Check if column base matches table name (singular/plural variants)
    if column_base:
        table_variants = _get_table_name_variants(ref_table_lower)
        if column_base in table_variants:
            return True
    
    return False


def _extract_base_name(name: str) -> str:
    """Extract the base name from a column name (remove _id, id suffixes)."""
    # Remove common suffixes
    for suffix in ['_id', 'id']:
        if name.endswith(suffix):
            return name[:-len(suffix)]
    return name


def _get_table_name_variants(table_name: str) -> List[str]:
    """Get possible variants of a table name (singular/plural, with/without underscores)."""
    variants = [table_name]
    
    # Remove common prefixes/suffixes
    clean_name = table_name
    if '_' in clean_name:
        # user_profiles -> profiles, user
        parts = clean_name.split('_')
        variants.extend(parts)
        # Also try without underscores
        variants.append(''.join(parts))
    
    # Try singular/plural variants
    if table_name.endswith('s') and len(table_name) > 1:
        singular = table_name[:-1]
        variants.append(singular)
    
    # Try adding 's' for plural
    if not table_name.endswith('s'):
        plural = table_name + 's'
        variants.append(plural)
    
    return list(set(variants))  # Remove duplicates


def _calculate_confidence(column_name: str, ref_pk: str, ref_table_name: str) -> float:
    """Calculate confidence score for a foreign key relationship (0.0 to 1.0)."""
    column_lower = column_name.lower()
    ref_pk_lower = ref_pk.lower()
    ref_table_lower = ref_table_name.lower()
    
    # Exact match = highest confidence
    if column_lower == ref_pk_lower:
        return 1.0
    
    # Column base matches ref pk base
    column_base = _extract_base_name(column_lower)
    ref_base = _extract_base_name(ref_pk_lower)
    
    if column_base == ref_base and column_base:
        return 0.95
    
    # Column base matches table name variants
    if column_base:
        table_variants = _get_table_name_variants(ref_table_lower)
        if column_base in table_variants:
            return 0.9
    
    # Partial matches get lower confidence
    if column_base and ref_base:
        if column_base in ref_base or ref_base in column_base:
            return 0.7
    
    # Default confidence for any match that made it this far
    return 0.5 