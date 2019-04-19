"""
Utilities for managing files, including HTTP downloads.
"""
import requests

def file_download(file_url, file_path):
    """
    Simple function for using requests module to download file from a URL if
    the file is available.
    """
    file_response = requests.get(file_url)
    with open(file_path, 'wb') as file:  
        file.write(file_response.content)
    return
    