"""
Class for loading a settings file, creating a nested dictionary, and enabling
lookups by user.
"""

from configparser import ConfigParser
from dataclasses import dataclass
import re

@dataclass
class Settings(object):

    import_path : object
    
    def __post_init__(self):   
        """ 
        Method for ConfigParser converting values to appropriate datatypes.
        """
        config = ConfigParser(dict_type = dict)
        config.optionxform = lambda option : option
        config.read(self.import_path)
        self.config = dict(config._sections)
        for section, nested_dict in self.config.items():
            for key, value in nested_dict.items():
                self.config[section][key] = self._typify(value)
        return self
    
    def _typify(self, value):
        """
        Method that converts strings to list (if comma present), int, float, 
        or boolean types based upon the content of the string imported from
        ConfigParser.
        """
        if ', ' in value:
            return value.split(', ')
        elif re.search('\d', value):
            try:
                return int(value)
            except ValueError:
                try:
                    return float(value) 
                except ValueError:
                    return value
        elif value in ['True', 'true', 'TRUE']:
            return True
        elif value in ['False', 'false', 'FALSE']:
            return False
        elif value in ['None', 'none', 'NONE']:
            return None
        else:
            return value
    
    def __getitem__(self, section, nested_key = None):
        if nested_key:
            return self.config[section][nested_key]
        else:
            return self.config[section]  
        
    def __setitem__(self, section, nested_dict):
        self.config.update({section, nested_dict})
        return self