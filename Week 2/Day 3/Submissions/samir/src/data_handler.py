import os

import urllib.request
import tarfile

PATH_WARNING = 'can\'t find the folder `%s`, the code will initialize it.'
PATH_EXCEPTION = 'can\'t find the zip file `%s`.'


class DataDownloader(object):
    def __init__(self, url):
        self.url = url

    def download_in(self, path):
        # download the data in the path
        urllib.request.urlretrieve(self.url, path)


class DataExtractor(object):
    def __init__(self, zip_path):
        self.tar_path = zip_path

    def unzip_to(self, path_to_unzip, mode='r:gz'):
        with tarfile.open(self.tar_path, mode=mode) as tar_file:
            tar_file.extractall(path_to_unzip)
