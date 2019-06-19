
from dataclasses import dataclass
import pandas as pd

from .external import External


@dataclass
class Legislature(External):
    """Contains information for loading and mapping legislative branch
    ideology variables.
    """

    jurisdiction : str = ''
    paths : object = None
    settings : object = None
    prep_message = 'Legislative ideology data prepared'
    wrangle_message = 'Legislative ideology data added to dataframe'

    def __post_init__(self):
        super().__post_init__()
        self.source_files = {'federal' : 'nominate_raw.csv'}
        self.prepped_files = {'federal' : 'nominate.csv'}
        self.prepper_options = {'federal' : self._munge_nominate}
        self._dicts = {'senate' : ['year', 'senate'],
                       'house' : ['year', 'house']}
        return

    def _munge_nominate(self):
        """Prepares political ideology data from the NOMINATE scores.
        """
        self.df = (self.df.assign(year1 = lambda x: x.congress*2 + 1787,
                                  year2 = lambda x: x.congress*2 + 1788)
                          .groupby(['congress', 'chamber'])
                          .median()
                          .reset_index())
        self.df = (pd.wide_to_long(self.df.reset_index(), stubnames = 'year',
                                   i = 'index', j = 'placeholder')
                     .reset_index(drop = True))
        self.df = self.df[self.df['year'] >= self.start_year]
        self.df = self.df[self.df['year'] <= self.end_year]
        sen_df = self.df[self.df['chamber'] == 'Senate']
        sen_df.rename(columns = {self.nom_score : 'senate'}, inplace = True)
        house_df = self.df[self.df['chamber'] == 'House']
        house_df.rename(columns = {self.nom_score : 'house'}, inplace = True)
        self.df = sen_df
        self.df = self.df.merge(house_df[['year', 'house']], on = ['year'])
        return

    def _set_source_variables(self):
        self.file_url = 'https://voteview.com/static/data/out/members/HSall_members.csv'
        self.source_columns = ['congress', 'chamber', 'nominate_dim1',
                               'nominate_dim2', 'nokken_poole_dim1',
                               'nokken_poole_dim2']
        self.prepped_columns = ['year', 'senate', 'house']
        self.renames = {}
        return self
