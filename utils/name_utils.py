"""
Name utilities for fuzzy matching and name variant generation.
"""

from typing import List


class NameUtils:
    """Utilities for name processing and fuzzy matching."""
    
    @staticmethod
    def get_name_variants(name: str) -> List[str]:
        """Get common name variants for fuzzy matching."""
        if name is None:
            return []
        
        name = name.lower().strip()
        variants = [name]  # Always include the original name
        
        # Common name variations
        name_variants = {
            'marc': ['mark'],
            'mark': ['marc'],
            'luis': ['louis'],
            'louis': ['luis'],
            'mukherjee': ['mukherjee'],  # Keep as is for now
            'd´elia': ['d?elia', 'd\'elia', 'delia'],  # Handle encoding variations
            'd?elia': ['d´elia', 'd\'elia', 'delia'],
            'd\'elia': ['d´elia', 'd?elia', 'delia'],
            'delia': ['d´elia', 'd?elia', 'd\'elia'],
            'kleiss': ['kleiss'],  # Keep as is for now
            'löwe': ['löwe', 'loewe'],  # Handle umlaut variations
            'loewe': ['löwe'],
            'titus': ['titus'],  # Keep as is for now
            'kleiss': ['kleiß'],  # Keep as is for now
            'kleis': ['kleiß'],  # Keep as is for now
            'kleiß': ['kleiss', 'kleis']  # Keep as is for now
        }
        
        if name in name_variants:
            variants.extend(name_variants[name])
        
        # Add encoding-normalized variants
        normalized_name = NameUtils.normalize_encoding(name)
        if normalized_name != name and normalized_name not in variants:
            variants.append(normalized_name)
        
        return variants
    
    @staticmethod
    def normalize_encoding(name: str) -> str:
        """Normalize common encoding variations in names."""
        # Handle common encoding issues
        encoding_variants = {
            'd´elia': 'delia',      # Smart quote to regular apostrophe
            'd?elia': 'delia',      # Question mark to regular apostrophe
            'd\'elia': 'delia',     # Regular apostrophe
            'd´': 'd\'',            # Smart quote to regular apostrophe
            'd?': 'd\'',            # Question mark to regular apostrophe
            'löwe': 'loewe',        # Umlaut to oe
            'ö': 'oe',              # Umlaut to oe
            'ü': 'ue',              # Umlaut to ue
            'ä': 'ae',              # Umlaut to ae
            'ß': 'ss'               # Sharp s to ss
        }
        
        normalized = name
        for variant, standard in encoding_variants.items():
            normalized = normalized.replace(variant, standard)
        
        return normalized
