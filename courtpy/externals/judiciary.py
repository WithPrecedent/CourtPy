
from dataclasses import dataclass

from .external import External


@dataclass
class Judiciary(External):
    """Contains information for loading and mapping judicial branch
    ideology variables.
    """
    jurisdiction : str = ''
    paths : object = None
    settings : object = None
    prep_message = 'Judicial ideology data prepared'
    wrangle_message = 'Judicial ideology data added to dataframe'

    def __post_init__(self):
        super().__post_init__()
        self.source_files = {'federal' : 'martin_quinn_raw.csv'}
        self.prepped_files = {'federal' : 'martin_quinn.csv'}
        self.prepper_options = {'federal' : self._munge_martin_quinn}
        self._dicts = {'min_ideology' : ['year', 'min'],
                       'med_ideology' : ['year', 'med'],
                       'max_ideology' : ['year', 'max']}
        return

    def _munge_martin_quinn(self):
        """This method prepares political ideology data from the Martin-Quinn
        scores. The files will be downloaded if mq_download is set to True.
        Downloading need only be done once and the flag should be subsequently
        changed to avoid harassing the host servers of the data.
        """
        self.df['year'] = (
                self.df['year'].str.replace(r'[a-zA-Z]', '').astype(int))
        self.df.groupby('year').mean().reset_index()
        self.df = self.df[self.df['year'] >= self.start_year]
        self.df = self.df[self.df['year'] <= self.end_year]
        self._export_prepped_file()
        return self

    def _set_source_variables(self):
        self.file_url = 'http://mqscores.lsa.umich.edu/media/2017/court.csv'
        self.source_columns = ['term', 'med', 'min', 'max']
        self.prepped_columns = ['year', 'med', 'min', 'max']
        self.renames = {'term' : 'year'}
        return self