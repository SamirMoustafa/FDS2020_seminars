import csv
import os
import time
import warnings

import dask
import dask.array as da
import dask.dataframe as dd
import dask_xgboost
import joblib
import pandas as pd
from dask import persist
from dask.distributed import Client
from dask.distributed import progress
from dask_ml.model_selection import train_test_split as train_test_split_dask
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.model_selection import train_test_split as train_test_split_normal

from src import DataDownloader, PATH_WARNING, DataExtractor

NO_FILES_EXCEPTION = 'can\'t find any files in the provided path %s'
file_to_save_path = 'summary_results.csv'


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


def list_of_df(csv_path, num_rows):
    return [read_ith_rows(prefix, num_rows) for prefix in get_files_path(csv_path)]


def concatenate_to_dask_from_np(np_list):
    n_samples_per_block = np_list[0].shape[0]
    n_features = np_list[0].shape[1]
    delayed = [dask.delayed(i) for i in np_list]
    arrays = [da.from_delayed(obj, shape=(n_samples_per_block, n_features), dtype=np_list[0].dtype) for obj in delayed]
    return da.concatenate(arrays, axis=0)


def concatenate_to_df_from_np(np_list):
    return pd.concat([pd.DataFrame(np_i) for np_i in np_list], axis=0)


def write_results_csv(file_name, headers_name, row_data, operation='a'):
    if len(headers_name) != len(row_data):
        raise ValueError('Row data length must match the file header length')
    _write_data = list()

    if not os.path.exists(file_name):
        operation = 'w'
        _write_data.append(headers_name)

    _write_data.append(row_data)

    with open(file_name, operation) as f:
        writer = csv.writer(f)
        _ = [writer.writerow(i) for i in _write_data]


def save_to_file(path, dict_saver):
    header = list(dict_saver.keys())
    values = list(dict_saver.values())
    write_results_csv(path, header, values)


def get_metric_dict(y_train, y_pred_train, y_test, y_pred_test):
    mean_squared_train = mean_squared_error(y_train, y_pred_train, )
    mean_squared_test = mean_squared_error(y_test, y_pred_test, )

    return {"mean_squared_error": (mean_squared_train, mean_squared_test)}


def run_single_model(model, params, X_train, X_test, y_train, y_test, is_dask=False):
    results = dict()
    clf_name = type(model).__name__

    clf_cv = GridSearchCV(model,
                          param_grid=params,
                          cv=StratifiedKFold(n_splits=5, shuffle=True, random_state=42),
                          scoring='neg_mean_squared_error',
                          n_jobs=-1)

    with joblib.parallel_backend("dask" if is_dask else 'loky'):
        clf_cv.fit(X_train, y_train)

    y_predict_train = clf_cv.best_estimator_.predict(X_train)
    y_predict_test = clf_cv.best_estimator_.predict(X_test)

    results[clf_name] = {"metric": get_metric_dict(y_train, y_predict_train,
                                                   y_test, y_predict_test, ),
                         "params": clf_cv.best_params_}
    return results, y_predict_train, y_predict_test


def get_dask_data(df, target_name):
    df = dd.from_pandas(df, chunksize=100)

    target = df[target_name]
    del df[target_name]  # Remove target  information from dataframe

    df, target = persist(df, target)  # Ask Dask to start work on these in the background
    progress(df, target)
    df = dd.get_dummies(df.categorize()).persist()
    X = df.to_dask_array(lengths=True)
    y = target.to_dask_array(lengths=True)
    return train_test_split_dask(X, y, test_size=.2, random_state=42)


def get_normal_data(df, target_name):
    target = df[target_name]
    del df[target_name]  # Remove target  information from dataframe
    df = pd.get_dummies(df)

    X = df.to_numpy()
    y = target.to_numpy()
    return train_test_split_normal(X, y, test_size=.2, random_state=42)


def task(df, ram_to_use, is_dask):
    client = None
    if is_dask:
        client = Client(threads_per_worker=10,
                        n_workers=10, memory_limit=''.join([str(ram_to_use), 'GB']))

    models = [Ridge(random_state=42),
              GradientBoostingRegressor(random_state=42), ][:1 if is_dask else 2]

    params = [{"alpha": [0.001, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0], },
              {'max_depth': [2, 3, 4, 6], 'n_estimators': [2, 3, 4, 5], }, ][:1 if is_dask else 2]

    X_train, X_test, y_train, y_test = get_dask_data(df.copy(), 'DepDelay') if is_dask else get_normal_data(df.copy(),
                                                                                                            'DepDelay')

    for model, param in zip(models, params):
        t_start = time.time()
        results, _, _ = run_single_model(model, param, X_train, X_test, y_train, y_test, is_dask=is_dask)
        model_name = type(model).__name__
        train_error, test_error = results[model_name]['metric']['mean_squared_error']
        t_end = time.time()
        time_took = round(t_end - t_start, 3)

        dict_saver = {}
        dict_saver.update({'model_name': model_name + ('_dask' if is_dask else '')})
        dict_saver.update({'train_error(MSE)': train_error})
        dict_saver.update({'test_error(MSE)': test_error})
        dict_saver.update({'time': time_took})
        save_to_file(file_to_save_path, dict_saver)

        print(model_name,
              ':\t took ->',
              time_took
              ,
              '\t with error (train, test)',
              (train_error, test_error))

    if is_dask:
        params = {'objective': 'reg:squarederror',
                  'max_depth': 4, 'eta': 0.01, 'subsample': 0.5,
                  'min_child_weight': 0.5}

        t_start = time.time()
        bst = dask_xgboost.train(client, params, X_train, y_train, num_boost_round=10)
        t_end = time.time()
        time_took = round(t_end - t_start, 3)

        y_train_hat = dask_xgboost.predict(client, bst, X_train).persist()
        y_test_hat = dask_xgboost.predict(client, bst, X_test).persist()

        y_train, y_train_hat = dask.compute(y_train, y_train_hat)
        y_test, y_test_hat = dask.compute(y_test, y_test_hat)

        train_error = mean_squared_error(y_train, y_train_hat)
        test_error = mean_squared_error(y_test, y_test_hat)

        dict_saver = {}
        dict_saver.update({'model_name': 'Dask XGBoost' + '_dask'})
        dict_saver.update({'train_error(MSE)': train_error})
        dict_saver.update({'test_error(MSE)': test_error})
        dict_saver.update({'time': time_took})
        save_to_file(file_to_save_path, dict_saver)

        print('Dask XGBoost',
              ':\t took ->',
              time_took,
              '\t with error (train, test)',
              (train_error, test_error))
