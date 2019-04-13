# CourtPy

CourtPy is a python package which allows a user to prepare, parse, wrangle, merge, engineer, analyze, and graph data derived from court opinions. It can download and integrate data from other sources including NOMINATE, Martin-Quinn, the FJC Integrated Database, and FJC Judicial Biography Database.

The package is presently in alpha and is nearly feature complete.

CourtPy is the master file that allows a user to apply any or all of the pipeline stages.

CourtPrepper takes raw court case files downloaded from Lexis-Nexis, CourtListener, and Caselaw Access and organizes them into the proper format and structure for the parser. It also downloads and/or prepares data from external sources to be merged with the case data.

CourtParser divides court opinions into core sections and searches for strings and regular expressions in the opinions.

CourtWrangler further parses and organizes the data while combining the data with biographical information about the judges and contextual political data.

CourtMerger allows the user to combine their data with other third-party data such as the Federal Judicial Center's Integrated Database or with the various case sources that have already been parsed.

CourtEngineer encodes, prepares, and feature engineers the data for machine learning algorithms.

CourtAnalyzer applies major machine learning models to the engineered data including algorithms (presently limited to sklearn models and others, such as xgboost, which have sklearn wrappers). GPU support is available for parallelization.

CourtPlotter graphs the findings of the analyzer using some common methods.

The package is designed to be accessible to non-python programmers. This has guided CourtPy's development from its inception. To that end, most of the the specific information (such as search terms and variables) is stored in .csv files that are used by the scripts. This allows the package to be used by people who need only use a .csv editor such as Microsoft Excel. Users can add their own search terms and variable names as well as remove default variables included in the package.

CourtPy will always be open-source and, consistent with the GitHub ethos, I welcome contributors.
