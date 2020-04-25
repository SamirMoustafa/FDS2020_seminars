from __future__ import print_function

import argparse

from src.utils import get_files_path, read_ith_rows, download_and_extract, handle_data_path

URL = "https://storage.googleapis.com/dask-tutorial-data/nycflights.tar.gz"
DATA_PATH = 'data'


def do_preprocessing(csv_path, num_rows):
    print("- Creating json data... ", end='', flush=True)

    for prefix in get_files_path(csv_path):
        # Just take the first 10000 rows for the demo
        df = read_ith_rows(prefix, num_rows)
        # pre-processing on the each data-frame

    print("done", flush=True)

    print("** Finished! **")


def main(args):
    print("Setting up data directory")
    print("-------------------------")

    tar_path, csv_path = handle_data_path(args.data, URL,)
    if download_and_extract(args.data, URL, tar_path):
        do_preprocessing(csv_path, args.num_rows)

    print('Finished!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='flights toy example')
    parser.add_argument('--data', default=DATA_PATH, type=str, help='path to download data in it.')
    parser.add_argument('--num_rows', default=10000, type=int, help='number of rows to read from each csv file.')
    args = parser.parse_args()
    main(args)
