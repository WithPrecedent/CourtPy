"""
Class of paths for use by CourtPy.
"""
from dataclasses import dataclass
import glob
import os
import random

from simplify import Filer

@dataclass
class Paths(Filer):

    settings : object
    root : str = ''
    data : str = ''
    results : str = ''
    experiment_folder : str = ''
    import_file : str = ''
    export_file : str = ''
    import_format : str = ''
    export_format : str = ''

    def __post_init__(self):
        """Builds upon default folder structure in simplify Filer."""
        if not self.root:
            self.root = os.path.join('..', '..', '..', 'data', 'courtpy')
        super().__post_init__()
        self.settings.localize(instance = self, sections = ['cases',
                                                            'general'])
        self.opinions = os.path.join(self.data, 'court_opinions',
                                     self.jurisdiction, self.case_type)
        self.raw_lexis_input = os.path.join(self.opinions, 'lexis_raw')
        self.lexis_input = os.path.join(self.opinions, 'lexis')
        self.raw_cl_input = os.path.join(self.opinions, 'court_listener_raw')
        self.cl_input = os.path.join(self.opinions, 'court_listener')
        self.raw_ca_input = os.path.join(self.opinions, 'caselaw_access_raw')
        self.ca_input = os.path.join(self.opinions, 'caselaw_access')
        self.dividers = 'dividers'
        self.mungers = 'mungers'
        self.externals = 'externals'
        self.biographies = os.path.join(self.externals, 'biographies',
                                        self.jurisdiction)
        self.courts = os.path.join(self.externals, 'courts',
                                   self.jurisdiction)
        self.executive = os.path.join(self.externals, 'executive',
                                      self.jurisdiction)
        self.judiciary = os.path.join(self.externals, 'judiciary',
                                      self.jurisdiction)
        self.legislature = os.path.join(self.externals, 'legislature',
                                        self.jurisdiction)
        self._make_io_folders()
        self._make_file_dicts()
        return self

    def _make_file_dicts(self):
        """Creates file names based upon the options selected in stages in
        settings.
        """
        self.import_files = {'wrangler' : 'parsed',
                             'merger' : 'wrangled',
                             'analyzer' : 'engineered',
                             'plotter' : 'analyzed'}
        if 'merge' in self.stages:
            self.import_files.update({'engineer' : 'merged'})
        else:
            self.import_files.update({'engineer' : 'wrangled'})
        self.export_files = {'parser' : 'parsed',
                             'wrangler' : 'wrangled',
                             'merger' : 'merged',
                             'engineer' : 'engineered',
                             'analyzer' : 'analyzed',
                             'plotter' : 'plotted'}
        return

    def _make_io_folders(self):
        """Makes needed paths that do not include files within the package in
        case they do not already exist.
        """
        if not os.path.exists(self.opinions):
             os.makedirs(self.opinions)
        if not os.path.exists(self.lexis_input):
             os.makedirs(self.lexis_input)
        if not os.path.exists(self.cl_input):
             os.makedirs(self.cl_input)
        if not os.path.exists(self.ca_input):
             os.makedirs(self.ca_input)
        return

    def conform(self, source = None, stage = None):
        """Creates import, export, and other paths based upon the data source
        and stage.
        """
        self.stage = stage
        self.source = source
        if self.verbose:
            if self.test_data:
                print('Using test data')
            else:
                print('Using full data')
        if self.stage in ['prepper']:
            if self.source == 'lexis_nexis':
                self.import_folder = self.lexis_raw_input
                self.export_path = self.lexis_input
                self.import_paths = glob.glob(os.path.join(self.import_folder,
                                                           '**', '*.txt'),
                                                           recursive = True)
        elif self.stage in ['parser']:
            self.export_file = (self.source
                                + '_'
                                + self.export_files[self.stage]
                                + '.csv')
            self.export_path = os.path.join(self.data, self.export_file)
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
            if self.test_data:
                if self.use_seed_test_chunk:
                    random.seed(self.seed)
                self.import_paths = random.sample(
                        self.import_paths,
                        self.test_chunk)
        elif self.stage in ['wrangler']:
            self.import_file = self.import_file = (
                    self.source + '_' + self.import_files[self.stage])
            self.export_file = self.export_file = (
                    self.source + '_' + self.export_files[self.stage])
        elif self.stage in ['engineer']:
            if 'merger' in self.stages:
                self.import_file = self.import_files[self.stage]
            else:
                self.import_file = (
                        self.source + '_' + self.import_files[self.stage])
            self.export_file = self.export_files[self.stage]
        elif self.stage in ['analyzer']:
            self.import_file = self.import_files[self.stage]
            self.export_file = self.export_files[self.stage]
        self._make_io_paths()
        return self