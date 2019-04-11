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
        self.output = os.path.join(self.data, 'output')
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
        self.data_in = self.settings['files']['data_in']
        self.data_out = self.settings['files']['data_out']
        self._make_io_paths()
        self._make_file_dicts()
        return self

    def _make_file_dicts(self):
        self.import_files = {'wrangle' : 'parsed',
                             'merge' : 'wrangled',
                             'analyze' : 'engineered',
                             'plot' : 'analyzed'} 
        if 'merge' in self.settings['general']['stages']:
            self.import_files.update({'engineer' : 'merged'})
        else:
            self.import_files.update({'engineer' : 'wrangled'})
        self.export_files = {'parse' : 'parsed',
                             'wrangle' : 'wrangled',
                             'merge' : 'merged',
                             'engineer' : 'engineered',
                             'analyze' : 'analyzed',
                             'plot' : 'plotted'}    
        return
    
    def _make_io_paths(self):
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
            self.export_file = self._file_name(
                    source = self.source,
                    name = self.export_files[self.stage], 
                    extension = self.data_out)     
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
            self.import_file = self.import_file = (
                    self.source + '_' + self.import_files[self.stage]) 
            self.export_file = self.export_file = (
                    self.source + '_' + self.export_files[self.stage])      
        elif self.stage in ['engineer']:
            if 'merge' in self.settings['general']['stages']:
                self.import_file = self.import_files[self.stage]
            else:
                self.import_file = (
                        self.source + '_' + self.import_files[self.stage]) 
            self.export_file = self.export_files[self.stage]
        elif self.stage in ['analyze', 'plot']:
            self.import_file = self.import_files[self.stage]
            self.export_file = self.export_files[self.stage]
        return self