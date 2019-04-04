"""
Class of paths for use by CourtPy.
"""
from dataclasses import dataclass
import os
import random

import glob

@dataclass
class Paths(object):
    
    settings : object
    
    def __post_init__(self): 
        self.data = os.path.join('..', '..', '..', 'data', 'courtpy')
        self.input = os.path.join(self.data, 'input')
        self.output = os.path.join(self.data, 'output', 
                                   self.settings['files']['output_folder'])
        self.opinions = os.path.join(self.input, 'court_opinions',
                                     self.settings['cases']['jurisdiction'],
                                     self.settings['cases']['case_type'])
        self.raw_lexis_input = os.path.join(self.opinions, 'lexis_raw')
        self.lexis_input = os.path.join(self.opinions, 'lexis')
        self.raw_cl_input = os.path.join(self.opinions, 'court_listener_raw')
        self.cl_input = os.path.join(self.opinions, 'court_listener')
        self.raw_ca_input = os.path.join(self.opinions, 'caselaw_access_raw')
        self.ca_input = os.path.join(self.opinions, 'caselaw_access')
        """
        Folder containing .csv files which can be edited to change the strings 
        and regular expressions used by CourtPy.
        """
        self.dicts = 'dictionaries'
        self.case_rules = os.path.join(self.dicts, 'case_rules.csv')
        self.make_io_paths()
        return
    
    def make_io_paths(self):
        """
        Makes needed paths that do not include files within the package in case 
        they do not already exist.
        """
        if not os.path.exists(self.data):
             os.makedirs(self.data)
        if not os.path.exists(self.input):
             os.makedirs(self.input)
        if not os.path.exists(self.output):
             os.makedirs(self.output)
        if not os.path.exists(self.lexis_input):
             os.makedirs(self.lexis_input)
        if not os.path.exists(self.cl_input):
             os.makedirs(self.cl_input)
        if not os.path.exists(self.ca_input):
             os.makedirs(self.ca_input)      
        return
    
    def conform(self, stage = None, source = None):     
        self.stage = stage
        self.source = source
        if self.settings['general']['verbose']:
            if self.settings['files']['test_data']:
                print('Using test data')
            else:
                print('Using full data')            
        if self.stage in ['prep']:
            if self.source == 'lexis_nexis':
                self.import_folder = self.lexis_raw_input
                self.export_path = self.lexis_input
                self.import_paths = glob.glob(os.path.join(self.import_folder, 
                                                           '**', '*.txt'),
                                                           recursive = True)            
        elif self.stage in ['parse']:
            self.export_path = self.path_constructor(False, new_format = 'csv')  
            if source == 'lexis_nexis':
                self.import_folder = self.lexis_input
                self.import_paths = glob.glob(os.path.join(self.import_folder, 
                                                            '**', '*.txt'),
                                                            recursive = True)
            elif source == 'court_listener':
                self.import_folder = self.cl_input
                self.import_paths = glob.glob(os.path.join(self.import_folder, 
                                                            '**', '*.json'),
                                                            recursive = True)
            elif source == 'caselaw_access':
                self.import_folder = self.ca_input
                self.import_paths = glob.glob(os.path.join(self.import_folder, 
                                                            '**', '*.json'),
                                                            recursive = True)
            if self.settings['files']['test_data']:
                if self.settings['files']['use_seed_test_chunk']:
                    random.seed(self.settings['general']['seed'])
                self.import_paths = random.sample(
                        self.import_paths, 
                        self.settings['files']['test_chunk'])
        elif self.stage in ['wrangle']:
            self.import_path = self.path_constructor(True, new_format = 'csv')
            self.export_path = self.path_constructor(False)
        elif self.stage in ['engineer', 'analyze', 'plot']:
            self.import_path = self.path_constructor(True)
            self.export_path = self.path_constructor(False)
        return self
    
    def path_constructor(self, get_import, new_format = None):
        if new_format:
            file_format = new_format
        elif get_import:
            file_format = self.settings['files']['data_in']
        else:
            file_format = self.settings['files']['data_out']
        if get_import:
            file_dict = {'wrangle' : 'parsed',
                         'merge' : 'wrangled',
                         'analyze' : 'engineered',
                         'plot' : 'analyzed'} 
            if 'merge' in self.settings['general']['stages']:
                file_dict.update({'engineer' : 'merged'})
            else:
                file_dict.update({'engineer' : 'wrangled'})
        else:
            file_dict = {'parse' : 'parsed',
                         'wrangle' : 'wrangled',
                         'merge' : 'merged',
                         'engineer' : 'engineered',
                         'analyze' : 'analyzed',
                         'plot' : 'plotted'}            
        stage_string = file_dict.get(self.stage)
        if self.stage in ['parse', 'wrangle']:
            new_path = ''.join([os.path.join(self.output, self.source + '_' 
                                             + stage_string), '.', 
                                             file_format])
        elif self.stage in ['engineer'] and get_import:
            new_path = ''.join([os.path.join(self.output, self.source + '_' 
                                             + stage_string), '.', 
                                             file_format])    
        elif self.stage in ['engineer', 'analyze', 'plot']:
            new_path = ''.join([os.path.join(self.output, stage_string), '.', 
                                file_format])
        return new_path