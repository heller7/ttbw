"""
Configuration management for the TTBW system.
"""

import yaml
from typing import Dict, Any


class ConfigManager:
    """Manages configuration loading and provides default values."""
    
    @staticmethod
    def load_config(config_file: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Configuration file '{config_file}' not found. Using default configuration.")
            return ConfigManager.get_default_config()
        except yaml.YAMLError as e:
            print(f"Error parsing configuration file: {e}. Using default configuration.")
            return ConfigManager.get_default_config()
    
    @staticmethod
    def get_default_config() -> Dict[str, Any]:
        """Return default configuration if config file is not available."""
        return {
            'default_birth_year': 2014,
            'age_classes': {
                2006: 19, 2007: 19, 2008: 19, 2009: 19,
                2010: 15, 2011: 15, 2012: 13, 2013: 13, 2014: 11
            },
            'districts': {
                'Hochschwarzwald': {'region': 1, 'short_name': 'HS'},
                'Ulm': {'region': 2, 'short_name': 'UL'},
                'Donau': {'region': 3, 'short_name': 'DO'},
                'Ludwigsburg': {'region': 4, 'short_name': 'LB'},
                'Stuttgart': {'region': 5, 'short_name': 'ST'}
            }
        }
