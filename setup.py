from setuptools import setup

__version__ = '0.1.0'

setup(name = 'courtpy',
      version = __version__,
      description = "CourtPy offers flexible, accessible tools for parsing and analysis of court opinions",
      author = "Corey Rayburn Yung",
      author_email = 'coreyyung@ku.edu',
      url = 'https://github.com/with_precedent/courtpy',
      packages = ['courtpy'],
      entry_points = {'console_scripts': ['courtpy = courtpy.cli:cli']},
      python_requires = '>= 3.6',
      install_requires = open('requirements.txt').readlines(),
      keywords = 'courtpy data science machine learning pandas sklearn',
      classifiers = ['Programming Language :: Python :: 3.6',
                     'Programming Language :: Python :: 3.7'])