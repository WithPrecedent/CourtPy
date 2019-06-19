
from dataclasses import dataclass
import os
import warnings

from simplify import Settings
from simplify import timer

from cases import Cases
from tool import Tool
from utilities.paths import Paths


@timer('Feature engineering')
@dataclass
class Engineer(Tool):
    """Engineers features of the data based upon user settings."""

    paths : object
    settings : object
    stage : str = 'engineer'
    _complete_message = 'Feature engineering complete'

    def __post_init__(self):
        super().__post_init__()
        if self.verbose:
            print('Beginning feature engineering')
        sources = self.check_sources()
        for source in sources:
            self.source = source
            self.quick_start()
            self.cases.add_unique_index()
            self.cull_data(drop_prefixes = self.cases.drop_prefixes)
            self.data = self.shape_df(self.data)
            self.data.summarize(export_path = os.path.join(
                    self.paths.data, 'summary_data.csv'))
            self.data.convert_rare(
                    cats = self.data.create_column_list(
                            prefixes = self.cases.cat_prefixes),
                    threshold = self.cat_threshold)
            self.data.drop_infrequent(
                    bools = self.data.create_column_list(
                            prefixes = self.cases.bool_prefixes),
                    threshold = self.drop_threshold)
            self.engineer_loose_ends(self.data.df)
            self.data.save(export_folder = self.paths.data,
                           file_name = self.paths.export_file,
                           file_format = self.export_format,
                           boolean_out = self.boolean_out,
                           encoding = self.encoding)
            self.loop_cleanup()
        return

if __name__ == '__main__':
    settings = Settings(os.path.join('settings', 'courtpy_settings.ini'))
    ml_settings = Settings(os.path.join('settings', 'simplify_settings.ini'))
    settings.update(ml_settings)
    paths = Paths(settings)
    if not settings['general']['pandas_warnings']:
        warnings.filterwarnings('ignore')
    CourtEngineer(paths, settings)