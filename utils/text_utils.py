"""
Text processing utilities for the TTBW system.
"""

import re


class TextUtils:
    """Utilities for text processing and normalization."""
    
    @staticmethod
    def replace_umlauts(text: str) -> str:
        """Replace German umlauts with their ASCII equivalents."""
        umlaut_map = {
            'ä': 'ae', 'ö': 'oe', 'ü': 'ue',
            'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue',
            'ß': 'ss'
        }
        
        for umlaut, replacement in umlaut_map.items():
            text = text.replace(umlaut, replacement)
        
        return text
    
    @staticmethod
    def normalize_name(name: str) -> str:
        """Normalize a name for consistent comparison."""
        if not name:
            return ""
        
        # Convert to lowercase and strip whitespace
        normalized = name.lower().strip()
        
        # Replace umlauts
        normalized = TextUtils.replace_umlauts(normalized)
        
        return normalized
    
    @staticmethod
    def normalize_club(club: str) -> str:
        """Normalize a club name for consistent comparison."""
        if not club:
            return ""
        
        # Convert to lowercase and strip whitespace
        normalized = club.lower().strip()
        
        # Replace umlauts
        normalized = TextUtils.replace_umlauts(normalized)
        
        return normalized
