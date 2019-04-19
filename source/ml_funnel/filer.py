"""
Class and methods used by ml_funnel to store data and results in consistent
path structure based upon user options.
"""
from dataclasses import dataclass
import os

@dataclass
class Filer(object):
    """
    Class for dynamically creating file paths.
    """   
    data_folder : str
    results_folder : str
    experiment_folder : str = ''
    import_file_name : str = ''
    export_file_name : str = ''
    settings : object = None
    
    def __post_init__(self):
        self._make_folder(self.data_folder)
        self._make_folder(self.results_folder)
        if self.import_file_name:
            self.data_file_in = self.make_path(
                    folder = self.data_folder,
                    name = self.import_file_name,
                    file_type = self.settings['files']['data_in'])
        if self.export_file_name:
            self.data_file_out = self.make_path(
                    folder = self.data_folder,
                    name = self.export_file_name,
                    file_type = self.settings['files']['data_out'])
        return self
    
    def make_path(self, folder = '', subfolder = '', prefix = '', 
                  name = '', suffix = '', file_type = 'csv'):
        if subfolder:
            folder = os.path.join(folder, subfolder)
        if name:
            file_name = self._file_name(prefix = prefix, 
                                        name = name,
                                        suffix = suffix,
                                        file_type = file_type)
            return os.path.join(folder, file_name)
        else:
            return folder
    
    def _file_name(self, prefix = '', name = '', suffix = '', file_type = ''):
        extensions = {'csv' : '.csv',
                      'pickle' : '.pkl',
                      'feather' : '.ftr',
                      'h5' : '.hdf',
                      'excel' : '.xlsx',
                      'text' : '.txt',
                      'xml' : '.xml',
                      'png' : '.png'}
        return prefix + name + suffix + extensions[file_type]
    
    def _make_folder(self, folder):
        if not os.path.exists(folder):
             os.makedirs(folder)
        return self
    
    def _iter_path(self, model, tube_num, splicer = '', file_name = '', 
                   file_type = ''):
        if splicer:
            subfolder = model.name + '_' + splicer.name + tube_num
        else:
            subfolder = model.name + tube_num
        self._make_folder(folder = self.make_path(
                folder = self.test_tubes_folder,
                subfolder = subfolder))
        return self.make_path(folder = self.test_tubes_folder,
                              subfolder = subfolder,
                              name = file_name,
                              file_type = file_type)
