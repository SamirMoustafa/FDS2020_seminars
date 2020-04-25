from __future__ import print_function

import os

from src.utils import get_files_path, read_ith_rows, download_and_extract

URL = "https://storage.googleapis.com/dask-tutorial-data/nycflights.tar.gz"

DATA_PATH = 'data'
TAR_PATH = os.path.join(DATA_PATH, URL.split('/')[-1])
CSV_PATH = os.path.join(DATA_PATH, 'nycflights')


def do_preprocessing(csv_path):
    print("- Creating json data... ", end='', flush=True)

    for prefix in get_files_path(csv_path):
        # Just take the first 10000 rows for the demo
        df = read_ith_rows(prefix, 10000)
        # pre-processing on the each data-frame

    print("done", flush=True)

    print("** Finished! **")


def main():
    print("Setting up data directory")
    print("-------------------------")

    if download_and_extract(DATA_PATH, URL, TAR_PATH):
        do_preprocessing(CSV_PATH)

    print('Finished!')


if __name__ == '__main__':
    main()
