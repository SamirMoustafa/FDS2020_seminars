import os
import warnings


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


def instances_from_df_to_json(df, json_name, num_of_instances):
    instances = df.iloc[:num_of_instances]
    instances.to_json(json_name, orient='records', lines=True)
    return True
