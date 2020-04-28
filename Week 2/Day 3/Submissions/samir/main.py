from __future__ import print_function

import os
import warnings

warnings.simplefilter("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"

import argparse

from src.utils import handle_data_path, download_and_extract, list_of_df, concatenate_to_df_from_np, task

URL = "https://storage.googleapis.com/dask-tutorial-data/nycflights.tar.gz"
DATA_PATH = 'data'

# Selected features
cols = ['Year', 'Month', 'DayOfWeek', 'Distance', 'DepDelay', 'CRSDepTime', 'UniqueCarrier', 'Origin', 'Dest']


def main(args):
    print("Setting up data directory")
    print("-------------------------")

    tar_path, csv_path = handle_data_path(args.data, args.url)
    download_and_extract(args.data, args.url, tar_path)
    df_list = list_of_df(csv_path, args.num_rows)

    # concatenate the data-frames and choose the features ana drop nan's
    df = concatenate_to_df_from_np(df_list)[cols].dropna()

    print('=' * 10, 'dask', '=' * 10)
    task(df, args.ram, is_dask=True)

    print('=' * 10, 'scikit', '=' * 10)
    task(df, args.ram, is_dask=False)

    print('Finished!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='flights toy example')
    parser.add_argument('--data', default=DATA_PATH, type=str, help='path to download data in it.')
    parser.add_argument('--num_rows', default=10, type=int, help='number of rows to read from each csv file.')
    parser.add_argument('--ram', default=2, type=float, help='capacity of the used RAM by dask.')
    parser.add_argument('--url', default=URL, type=str, help='link to download dataset from it.')
    args = parser.parse_args()
    main(args)
