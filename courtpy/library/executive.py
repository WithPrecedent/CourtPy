"""
Class related to political data from executive branches.
"""

from dataclasses import dataclass
import os
from shutil import copyfile

from utilities.dict_creator import CSVDict

@dataclass
class Executive(object):
    
    paths : object
    settings : object
    
    def __post_init__(self):
        self.executive_path = os.path.join(self.paths.input, 'executive')
        return
    
    def test_opposite(self, judge_party):
        if int(self.ideo) * int(judge_party) > 0:
            self.opposite = False
        elif int(self.ideo) * int(judge_party) < 0:
            self.opposite = True
        else:
            self.opposite = ''   
        return
    
@dataclass
class President(Executive):
    
    paths : object
    settings : object
    
    def __post_init__(self):
        
        super().__post_init__()
        self.federal_executive_path = os.path.join(self.executive_path, 
                                                  'federal')
        self.pres_import = os.path.join(self.paths.dicts, 'president.csv')
        self.pres_export = os.path.join(self.federal_executive_path, 
                                        'president.csv')
        return
    
    def make_paths(self):
        if not os.path.exists(self.executive_path):
             os.makedirs(self.executive_path)
        if not os.path.exists(self.federal_executive_path):
             os.makedirs(self.federal_executive_path)
        return self
             
    def make_file(self):
        copyfile(self.pres_import, self.pres_export)
        return
    
    def import_file(self):
        self.ideo = CSVDict(self.pres_export, 'str_str').lookup
        return self
    
