"""
Classes related to political data from legislatures.
"""

from dataclasses import dataclass
import os
import pandas as pd

from utilities.dict_creator import CSVDict
from utilities.files import file_download

@dataclass
class Legislature(object):
     
    paths : object
    settings : object
    
    def __post_init__(self): 
        self.legis_path = os.path.join(self.paths.input, 'legislative')
        return
    
    def test_opposite(self, judge_party):
        if int(self.house_ideo) * int(judge_party) > 0:
            self.opposite_house = False
        elif int(self.house_ideo) * int(judge_party) < 0:
            self.opposite_house = True
        else:
            self.opposite_house = '' 
        if int(self.sen_ideo) * int(judge_party) > 0:
            self.opposite_sen = False
        elif int(self.sen_ideo) * int(judge_party) < 0:
            self.opposite_sen = True
        else:
            self.opposite_sen = '' 
        return
    
@dataclass
class Congress(Legislature):   
    """
    This class prepares political ideology data from the NOMINATE scores. The 
    NOMINATE files will be downloaded if nom_download is set to True. 
    Downloading need only be done once and the flag should be subsequently 
    changed to avoid harassing the host servers of the data.
    """     
    paths : object
    settings : object

    def __post_init__(self): 
        super().__post_init__()
        self.federal_legis_path = os.path.join(self.legis_path, 'federal')
        self.nom_url = 'https://voteview.com/static/data/out/members/HSall_members.csv'
        self.nom_import = os.path.join(self.federal_legis_path, 
                                       'nominate_raw.csv')
        self.sen_nom_export = os.path.join(self.federal_legis_path, 
                                           'sen_nominate.csv')  
        self.house_nom_export = os.path.join(self.federal_legis_path, 
                                             'house_nominate.csv')  
        return
    
    def make_paths(self):
        if not os.path.exists(self.legis_path):
             os.makedirs(self.legis_path)
        if not os.path.exists(self.federal_legis_path):
             os.makedirs(self.federal_legis_path)
        return self
    
    def make_file(self):
        self.nominate_source = ['congress', 'chamber', 'nominate_dim1',
                                 'nominate_dim2', 'nokken_poole_dim1', 
                                 'nokken_poole_dim2']
        self.nominate_out = ['year', 'nominate_dim1', 'nominate_dim2', 
                             'nokken_poole_dim1', 'nokken_poole_dim2']
        if self.settings['prepper']['nom_download']:
            file_download(self.nom_url, self.nom_import)
        df = (pd.read_csv(self.nom_import,
                          usecols = self.nominate_source,
                          index_col = False)
                .dropna(subset = ['nokken_poole_dim1'])
                .assign(year1 = lambda x: x.congress*2 + 1787,
                        year2 = lambda x: x.congress*2 + 1788)
                .groupby(['congress', 'chamber']).median().reset_index())
        df = (pd.wide_to_long(df.reset_index(), stubnames = 'year',
                              i = 'index', j = 'placeholder')
                .reset_index(drop = True))
                
        df = df[df['year'] >= self.settings['general']['start_year']]
        df = df[df['year'] <= self.settings['general']['end_year']]
                
        sen_df = df[df['chamber'] == 'Senate']
        sen_df.to_csv(self.sen_nom_export, 
                      columns = self.nominate_out,
                      index = False)
        
        house_df = df[df['chamber'] == 'House']  
        house_df.to_csv(self.house_nom_export, 
                        columns = self.nominate_out,
                        index = False)
        return
    
    def import_file(self):
        self.house_ideo = CSVDict(
                self.house_nom_export, 
                'str_str', 
                keys = 'year', 
                values = self.settings['wrangler']['nom_score']).lookup
        self.sen_ideo = CSVDict(
                self.sen_nom_export, 
                'str_str', 
                keys = 'year',
                values = self.settings['wrangler']['nom_score']).lookup
        return self
