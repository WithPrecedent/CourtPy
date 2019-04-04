"""
This module divides batch downloaded files from Lexis-Nexis into .txt files.
Each .txt file will contain one court opinion. This is done to reduce
memory usage in the parsing modules so that one case can be parsed at a time
without other cases also being in memory. The make_subfolders option is 
included if the user wants the individual case files to be stored in 
separate subdirectories based upon which court the opinion was issued in.
"""

import os
import re

from utilities.strings import no_double_space

def lexis_splitter(paths, settings):
    
    """
    This is the function that should be called for dividing batch Lexis files 
    inorder to parse them into individual text files. This process only needs 
    to be completed once and should not be repeatedly called by the main
    CourtPy package. After splitting the files once, the flag lexis_split
    should be changed to False.
    """
    subfolders = {1 : 'first_circuit', 2 : 'second_circuit', 
                  3 : 'thirrd_circuit', 4 : 'fourth_circuit', 
                  5 : 'fifth_circuit', 6 : 'sixth_circuit',
                  7 : 'seventh_circuit', 8 : 'eighth_circuit', 
                  9 : 'ninth_circuit', 10 : 'tenth_circuit', 
                  11 : 'eleventh_circuit', 12 : 'dc_circuit', 
                  13 : 'federal_circuit', 101 : 'bankruptcy_appeal', 
                  102 : 'temporary_emergency', 999 : 'other_court'}
    
    paths.make_io_paths()
    paths.set_io_paths()
    if settings.make_subfolders:
        for key, value in subfolders.items(): 
            i_path = os.path.join(paths.export_path, value)
            if not os.path.exists(i_path):
                os.makedirs(i_path)
    
    tot_index = 1
    for each_path in paths.import_paths:
        with open(each_path, mode = 'r', errors = 'ignore', 
                  encoding = settings.ext_encoding) as i_file:
            i_cases = i_file.read()
            i_cases = clean_file(i_cases)
            i_list = re.split('\d* of \d* DOCUMENTS', i_cases)
            cases_export(i_list, tot_index, dicts.court_nums, 
                              dicts.court_id, subfolders, paths,
                              settings)
    return

def clean_file(text):
    """
    This is a quick and dirty function for removing common anomalies in Lexis
    files.
    """
    text = text.replace('*', '')
    text = re.sub(r'\[\d*\]', '', text)
    text = re.sub(r'[^\S\n]', ' ', text)
    text = no_double_space(text)
    text = re.sub(r'(?m)^Signal:.*\n?\n?', '', text)
    text = re.sub(r'(?m)^SIGNAL:.*\n?\n?', '', text)
    text = re.sub(r'(?m)^As of:.*\n?\n?', '', text)
    text = re.sub(r'(?m)^AS OF:.*\n?\n?', '', text)
    text = re.sub(r'\n ', '\n', text)
    return text

def cases_export(i_list, tot_index, court_nums, court_id, subfolders, 
                 paths, settings):
    """
    This is the main function for exporting parsed court opinions into the 
    proper folders.
    """
    court_num = 0
    for i, case in enumerate(i_list):
        if len(case.strip()) > 0:
            if re.search(court_id.get('court_div'), case):
                court_sec = re.search(court_id.get('court_div'), case)
                court_num = match_iter(court_sec.upper, court_nums)
            else:
                court_num = 999
            subfolder = subfolders.get(court_num)
            file_name = str(court_num * 10000000 + tot_index) + '.txt'
            if settings.make_subfolders:
                path_name = os.path.join(paths.lexis_cases, subfolder, 
                                         file_name)
            else:
                path_name = os.path.join(paths.lexis_cases, file_name)    
            f = open(path_name, 'w')
            f.write(case.strip())
            f.close()
            tot_index += 1 
    return tot_index