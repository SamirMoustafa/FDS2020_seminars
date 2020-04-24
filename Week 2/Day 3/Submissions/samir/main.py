from __future__ import print_function

import os
from glob import glob

import pandas as pd

from src.data_handler import DataDownloader, DataExtractor

data_dir = 'data'


def flights():
    flights_raw = os.path.join(data_dir, 'nycflights.tar.gz')
    jsondir = os.path.join(data_dir, 'flightjson')

    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    url = "https://storage.googleapis.com/dask-tutorial-data/nycflights.tar.gz"

    data_downloader = DataDownloader(url)
    data_downloader.download_in(flights_raw)

    tar_path = os.path.join(data_dir, 'nycflights.tar.gz')

    data_extractor = DataExtractor(tar_path)
    data_extractor.unzip_to(data_dir)

    print("done", flush=True)

    csv_path = os.path.join('data', 'nycflights', '*.csv')

    if not os.path.exists(jsondir):
        print("- Creating json data... ", end='', flush=True)
        os.mkdir(jsondir)
        for path in glob(os.path.join('data', 'nycflights', '*.csv')):
            prefix = os.path.splitext(os.path.basename(path))[0]
            # Just take the first 10000 rows for the demo
            df = pd.read_csv(path).iloc[:10000]
            df.to_json(os.path.join('data', 'flightjson', prefix + '.json'),
                       orient='records', lines=True)
        print("done", flush=True)

    print("** Finished! **")


def main():
    print("Setting up data directory")
    print("-------------------------")

    flights()
    # random_array()
    # weather()

    print('Finished!')


if __name__ == '__main__':
    main()
