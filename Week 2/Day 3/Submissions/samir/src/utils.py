import os
import warnings

import pandas as pd

from src import DataDownloader, PATH_WARNING, DataExtractor

NO_FILES_EXCEPTION = 'can\'t find any files in the provided path %s'


def director_handler(path, warning_str=None, exception_str=None):
    if warning_str and exception_str:
        raise ValueError('can\'t parse to the function two messages (warning and exception)')

    if not os.path.exists(path):

        if warning_str:
            warnings.warn(warning_str % path, Warning)

        os.mkdir(path)

        if exception_str:
            raise FileNotFoundError(exception_str % path)

    return True


def get_files_path(path):
    files_name_list = os.listdir(path)
    if not files_name_list:
        raise ValueError(NO_FILES_EXCEPTION % path)
    return [os.path.join(path, file_name) for file_name in files_name_list]


def read_ith_rows(df_path, num_of_instances):
    return pd.read_csv(df_path).head(num_of_instances)


def download_and_extract(data_dir, url, tar_path):
    director_handler(data_dir, warning_str=PATH_WARNING)

    data_downloader = DataDownloader(url)
    data_downloader.download_in(tar_path)

    data_extractor = DataExtractor(tar_path)
    data_extractor.unzip_to(data_dir)

    return True


def handle_data_path(path, url):
    TAR_PATH = os.path.join(path, url.split('/')[-1])
    CSV_PATH = TAR_PATH.split('.')[0]
    return TAR_PATH, CSV_PATH
