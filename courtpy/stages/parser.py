
import csv
from dataclasses import dataclass
import os
import warnings

from simplify import Settings, timer

from stage import Stage
from tools.opinion_divider import OpinionDivider
from tools.opinion_munger import OpinionMunger
from tools.paths import Paths


@timer('Initial case data collection and parsing')
@dataclass
class Parser(Stage):
    """Divides and parsers court opinions into pandas series and saves results
    as csv file.

    Attributes:
        paths: an instance of Paths.
        settings: an instance of Settings.
        data_source: database from which data is gathered. Current formats
            supported are 'caselaw_access', 'court_listener', 'fjc_idb',
            and 'lexis_nexis'.
        jurisdiction: 'federal' or postal code abbreviation of state.
        case_type: 'high_court', 'appellate', 'trial', or 'other'.

    All data extraction from the opinion text (and not headers or metadata) is
    performed by Parser. Other data is munged by the CourtWrangler in a
    vectorized manner. Because of the memory necessary to load all court
    opinions in large datasets, CourtParser has to apply non-vectorized,
    case-by-case methods.
    """
    paths : object = None
    settings : object = None
    data_source : str = ''
    jurisdiction : str = ''
    case_type : str = ''
    stage : str = 'parser'
    _start_message = 'Beginning importation and parsing of'
    _complete_message = 'importing and parsing complete'

    def __post_init__(self):
        """The main input/output loop which takes data from source files and
        outputs to a .csv file one case at a time. During parsing, the data is
        stored in a pandas series. The loop iterates through a globbed list of
        import paths.
        """
        super().__post_init__()
        for self.source in self._listify(self.sources):
            self.quick_start()
            self.divider = OpinionDivider(column_dict = self.cases.column_dict)
            self.munger = OpinionMunger()
            with open(self.paths.export_path, mode = 'w', newline = '',
                      encoding = self.encoding) as output_file:
                writer = csv.writer(output_file, dialect = 'excel')
                column_list = self.cases.column_dict.keys()
                writer.writerow(column_list)
                for case_num, a_path in enumerate(self.paths.import_paths):
                    with open(a_path, mode = 'r', errors = 'ignore',
                              encoding = self.encoding) as a_file:
                        bundle = {'header' : '', 'opinions' : '',
                                  'opinions_breaks' : '', 'a_path' : a_path,
                                  'cl_id' : 0, 'cl_url' : ''}
                        case_text = a_file.read()
                        self.cases.initialize_series()
                        self.cases.add_index(index_number = case_num + 1)
                        self.cases.df, bundle = self.divider.separate_header(
                                df = self.cases.df,
                                case_text = case_text,
                                bundle = bundle)
                        self.cases.df, bundle = self.divider.iterate(
                                df = self.cases.df,
                                bundle = bundle)
                        self.cases.df, bundle = self.parser.iterate(
                                df = self.cases.df,
                                bundle = bundle)
                        self.parsers.writer.writerow(self.cases.df)
                        if (case_num + 1) % 100 == 0 and self.verbose:
                            print(case_num + 1, 'cases parsed')
        return self

if __name__ == '__main__':
    settings = Settings(os.path.join('settings', 'courtpy_settings.ini'))
    ml_settings = Settings(os.path.join('settings', 'simplify_settings.ini'))
    settings.update(ml_settings)
    paths = Paths(settings)
    warnings.filterwarnings('ignore')
    Parser(paths, settings)