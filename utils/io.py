class IO(object):

    import csv
    import joblib
    import json
    import os
    import pandas as pd
    import random
    import time

    from datetime import datetime
    from os import path
    from pathlib import Path

    MAIN_DIRECTORY = os.path.dirname(__file__).replace("/utils", "")

    # Directories and file systems

    def abs_path(self, *path):
        return self.path.join(self.MAIN_DIRECTORY, *path)

    def path_join(self, *path):
        return self.path.join(*path)

    def exists(self, path):
        return self.os.path.isfile(path)

    def remove(self, path):
        self.os.remove(path)

    def make_dir(self, directory):
        self.Path(directory).mkdir(parents=True, exist_ok=True)

    def dir(self, path):
        return self.os.path.dirname(path)

    def base(self, path):
        return self.os.path.basename(path)

    def dir_and_base(self, path):
        return self.os.path.dirname(path), self.os.path.basename(path)

    def file_counter(self, directory, pattern='*'):
        counter = 0
        for i in self.Path(directory).glob(pattern):
            counter += 1
        return counter

    def pause(self, sec=random.randint(3, 7) / 10):
        return self.time.sleep(sec)

    # Time

    def now(self):
        return self.datetime.now().replace(microsecond=0)

    # Operations with files (open, write, read)

    def read_pq(self, path):
        if self.exists(path):
            return self.pd.read_parquet(path, engine="fastparquet")
        else:
            file_name = path.split('/')[-1]
            print(f'\nSorry, file {file_name} does not exist.')

    def squash_pq(self, directory, pattern='*.parquet'):
        return self.pd.concat(self.pd.read_parquet(parquet_file)
                              for parquet_file in self.Path(directory).glob(pattern)).reset_index(drop=True)

    def save_pkl(self, model, path=None, directory=None, name=None):
        directory = self.dir(path) if not directory and path else directory
        self.make_dir(directory)
        path = self.path_join(directory, name) if not path else path
        self.joblib.dump(model, path)

    def save_pq(self, data, path=None, directory=None, name=None):
        directory = self.dir(path) if not directory else directory
        self.make_dir(directory)
        path = self.path_join(directory, name) if not path else path
        data.to_parquet(path, compression='gzip', engine="fastparquet")

    def save_json(self, data, path=None, directory=None, name=None):
        directory = self.dir(path) if not directory else directory
        self.make_dir(directory)
        path = self.path_join(directory, name) if not path else path
        with open(path, 'w') as f:
            self.json.dump(data, f)
        f.close()

    def read_csv(self, path, delimiter=';'):
        return self.pd.read_csv(path, delimiter=delimiter)

    def create_csv(self, path, header, delimiter=';'):
        with open(path, 'w') as file:
            writer = self.csv.writer(file, delimiter=delimiter)
            writer.writerow(header)

    def append_csv(self, path, fields, delimiter=';'):
        with open(path, 'a') as file:
            writer = self.csv.writer(file, delimiter=delimiter)
            writer.writerow(fields)

    # Conversions

    def json_to_df(self, path):
        with open(path) as json_file:
            data = self.json.load(json_file)
        return self.pd.DataFrame(data)
