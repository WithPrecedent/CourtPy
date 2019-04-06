""" 
Master control script for CourtPy.
Each major module can be called individually or through this class. 
The CourtPy class allows the user to call a group of modules through an ad hoc 
workflow or run the complete pipeline. The user determines which parts of the 
pipeline to invoke by changing the options in settings.ini.

If there are any problems with this file or packaged modules, please contact 
the creator directly: coreyyung@ku.edu or post on the GitHub page.
Also, if you find any errors or ways to increase efficiency, please email 
or contribute.

If you utilize any code for published work, please use the citation included 
on the WithPrecedent Github page. Acknowledgement is greatly appreciated.
"""
from dataclasses import dataclass
import os
import warnings

from library.paths import Paths
from ml_funnel.settings import Settings
from utilities.timer import timer

"""
Main class that can prepare, parse, wrangle, merge, analyze, and/or
plot court opinion data.
"""
@timer('CourtPy')
@dataclass
class CourtPy(object):

    paths : object
    settings : object
     
    def __post_init__ (self):
        """            
        Class invocations for the main sections of the CourtPy pipeline: 
            1) CourtPrepper;
            2) CourtParser;
            3) CourtWrangler;
            4) CourtMerger;
            5) CourtEngineer; 
            6) CourtAnalyzer; and,
            7) CourtPlotter.
        
        To conserve memory, specific packages are imported within the
        conditional statements instead of at the beginning of the module.
        """
        if 'prep' in self.settings['general']['stages']:
            """ 
            ExternalData creates dictionaries from external sources to 
            be added into the dataset.
            Lexis_Splitter divides groups of downloaded Lexis cases into
            individual .txt files. If lexis_split = False, the files will
            not be split.
            """
            from CourtPrepper import CourtPrepper
            CourtPrepper(paths = self.paths, 
                         settings = self.settings)
            if self.settings['general']['verbose']:
                print('Preparation complete')
            
        if 'parse' in self.settings['general']['stages']:
            """
            CourtParser separates court opinions into the key sections based
            upon the source format.
            """
            from CourtParser import CourtParser  
            CourtParser(paths = self.paths, 
                        settings = self.settings)
            if self.settings['general']['verbose']:
                print('Data collection and parsing complete')
            
        if 'wrangle' in self.settings['general']['stages']:
            """
            CourtWrangler further parses string data stored in pandas dataframe
            created by CourtParser.
            """
            from CourtWrangler import CourtWrangler  
            CourtWrangler(paths = self.paths, 
                          settings = self.settings)
            if self.settings['general']['verbose']:
                print('Deep parsing and data wrangling complete')
        
        if 'merge' in self.settings['general']['stages']:
            """
            CourtMerger merges the prepped and parsed database with select
            preexisting third-party databases.
            """
            from CourtMerger import CourtMerger
            CourtMerger(paths = self.paths, 
                        settings = self.settings)
            if self.settings['general']['verbose']:
                print('Data merging complete')
          
        if 'engineer' in self.settings['general']['stages']:
            """
            CourtEngineer stages the data for input into common machine
            learning packages.
            """
            from CourtEngineer import CourtEngineer
            CourtEngineer(paths = self.paths, 
                          settings = self.settings)
            if self.settings['general']['verbose']:
                print('Feature engineering complete')
   
        if 'analyze' in self.settings['general']['stages']:
            """
            CourtAnalyzer performs select machine learning algorithms to the 
            data.
            """
            from CourtAnalyzer import CourtAnalyzer 
            CourtAnalyzer(paths = self.paths, 
                          settings = self.settings)
            if self.settings['general']['verbose']:
                print('Data analysis complete')
        
        if 'plot' in self.settings['general']['stages']:
            """
            CourtPlotter automatically plots specific results from generated
            by CourtAnalyzer.
            """
            from CourtPlotter import CourtPlotter
            CourtPlotter(paths = self.paths, 
                         settings = self.settings)
            if self.settings['general']['verbose']:
                print('Plotting complete')
        return  

if __name__ == '__main__':
    settings = Settings(os.path.join('..', 'settings.ini'))
    paths = Paths(settings)
    ml_settings = Settings(os.path.join('ml_funnel', 'ml_settings.ini'))
    settings.config.update(ml_settings.config)   
    if not settings['general']['warnings']:
        warnings.filterwarnings('ignore')
    CourtPy(paths, settings)   