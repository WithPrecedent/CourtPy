"""
Classes related to political data for judicial branch.
"""

from dataclasses import dataclass
import os
import pandas as pd

from utilities.dict_creator import CSVDict
from utilities.files import file_download

@dataclass
class HighCourt(object):
    
    paths : object
    settings : object
    
    def __post_init__(self): 
        self.judicial_path = os.path.join(self.paths.input, 'judicial')
        return

    def test_opposite(self, judge_party):
        if self.med_ideo:
            if int(self.med_ideo) * int(judge_party) > 0:
                self.opposite = False
            elif int(self.med_ideo) * int(judge_party) < 0:
                self.opposite = True
            else:
                self.opposite = ''
        else:
            if int(self.ideo) * int(judge_party) > 0:
                self.opposite = False
            elif int(self.ideo) * int(judge_party) < 0:
                self.opposite = True
            else:
                self.opposite = ''
        return

@dataclass
class SCOTUS(HighCourt):
    """
    This class prepares political ideology data from the Martin-Quinn scores. 
    The files will be downloaded if mq_download is set to True. Downloading 
    need only be done once and the flag should be subsequently changed to avoid 
    harassing the host servers of the data.
    """    
    paths : object
    settings : object
    
    def __post_init__(self):
        super().__post_init__()
        self.federal_judicial_path = os.path.join(self.judicial_path, 
                                                  'federal')
        self.mq_url = 'http://mqscores.lsa.umich.edu/media/2017/court.csv'
        self.mq_import = os.path.join(self.federal_judicial_path, 
                                      'martin_quinn_raw.csv')
        self.mq_export = os.path.join(self.federal_judicial_path, 
                                      'martin_quinn.csv')
        return self
    
    def make_paths(self):
        if not os.path.exists(self.judicial_path):
             os.makedirs(self.judicial_path)
        if not os.path.exists(self.federal_judicial_path):
             os.makedirs(self.federal_judicial_path)
        return self
    
    def make_file(self):
        self.mq_source = ['term', 'med', 'min', 'max']
        self.mq_renames = {'term' : 'year'}
        self.mq_out = ['year', 'med', 'min', 'max']
        if self.settings['prepper']['mq_download']:
            file_download(self.mq_url, self.mq_import)  
        df = (pd.read_csv(self.mq_import,
                          usecols = self.mq_source,
                          index_col = False)
                .rename(columns = self.mq_renames))
        df['year'] = df['year'].str.replace(r'[a-zA-Z]','').astype(int)
        df.groupby('year').mean().reset_index()       
        df = df[df['year'] >= self.settings['general']['start_year']]
        df = df[df['year'] <= self.settings['general']['end_year']]
        df.to_csv(self.mq_export, 
                  columns = self.mq_out,
                  index = False)
        return self
    
    def import_file(self):
        self.med_ideo = CSVDict(self.mq_export, 'str_str', keys = 'year', 
                                    values = 'med').lookup
        self.min_ideo = CSVDict(self.mq_export, 'str_str', keys = 'year', 
                                    values = 'max').lookup
        self.max_ideo = CSVDict(self.mq_export, 'str_str', keys = 'year', 
                                    values = 'min').lookup
        return self