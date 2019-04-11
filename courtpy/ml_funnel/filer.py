"""
Class for dynamically creating file paths.
"""
from dataclasses import dataclass
import os

@dataclass
class Filer(object):
    
    root_import : str
    root_export : str
    data_import : str = ''
    data_export : str = ''
    settings : object = None
    
    def __post_init__(self):
        self.import_folder = self.make_path(
                folder = self.root_import,
                subfolder = self.settings['files']['input_folder'])
        self.export_folder = self.make_path(
                folder = self.root_export,
                subfolder = self.settings['files']['output_folder'])
        self._make_folder(self.import_folder)
        self._make_folder(self.export_folder)
        if self.data_import:
            self.data_file_in = self.make_path(
                    folder = self.root_import,
                    subfolder = self.settings['files']['input_folder'],
                    name = self.data_import,
                    file_type = self.settings['files']['data_in'])
        if self.data_export:
            self.data_file_out = self.make_path(
                    folder = self.root_export,
                    subfolder = self.settings['files']['output_folder'],
                    name = self.data_export,
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
        self._make_folder(folder = self.make_path(folder = self.export_folder,
                                                  subfolder = subfolder))
        return self.make_path(folder = self.export_folder,
                              subfolder = subfolder,
                              name = file_name,
                              file_type = file_type)
