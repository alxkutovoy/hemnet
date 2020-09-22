class Data(object):

    import csv
    import os
    import pandas as pd

    from tqdm import tqdm

    from utils.io import IO

    MAIN_DIRECTORY = os.path.dirname(__file__).replace("/utils", "")

    SITEMAP_PATH = 'data/sitemap/sitemap.parquet'
    PAGES_PATH = 'data/pages/pages.parquet'
    CONTENT_PATH = 'data/content/content.parquet'

    SUBSET_PATH = 'data/dataset/raw/data.parquet'

    SL_PROCESSED_PATH = 'data/library/public_transport/sl.parquet'

    DESTINATIONS_PATH = 'data/dataset/features/destinations.parquet'
    ENTITIES_PATH = 'data/dataset/features/entities.parquet'
    TRANSPORT_PATH = 'data/dataset/features/transport.parquet'

    ENRICHED_SUBSET_PATH = 'data/dataset/enriched/data.parquet'

    PROPERTY_CLUSTERING_REPORT = 'data/dataset/reporting/property_clustering.png'
    FEATURE_IMPORTANCE_REPORT = 'data/dataset/reporting/feature_importance.png'
    FEATURES = 'data/dataset/processed/operations/features.txt'
    EDA = 'data/eda/data_profiling_report.html'
    HYPERPARAMETERS = 'data/dataset/processed/operations/bayesian_hyperparameters.json'
    MODEL = 'output/model/model.pkl'

    POINTS_JSON_PATH = 'resource/points.json'
    ENTITIES_JSON_PATH = 'resource/entities.json'
    EPICENTERS_JSON_PATH = 'resource/epicenters.json'
    SL_STOPS_PATH = 'data/library/public_transport/stops.json'
    SL_ROUTES_PATH = 'data/library/public_transport/routes.json'

    GMAPS_UNPROCESSED_PATH = 'data/library/gmaps/unprocessed.parquet'
    GMAPS_PROCESSED_PATH = 'data/library/gmaps/processed.parquet'

    FEATURES_METADATA = 'resource/features_metadata.csv'

    PREPROCESSED_DIR = 'data/dataset/processed'

    X_TRAIN_PATH = 'data/dataset/processed/x_train'
    Y_TRAIN_PATH = 'data/dataset/processed/y_train'
    E_TRAIN_PATH = 'data/dataset/processed/e_train'

    X_VAL_PATH = 'data/dataset/processed/x_val'
    Y_VAL_PATH = 'data/dataset/processed/y_val'
    E_VAL_PATH = 'data/dataset/processed/e_val'

    X_TEST_PATH = 'data/dataset/processed/x_test'
    Y_TEST_PATH = 'data/dataset/processed/y_test'
    E_TEST_PATH = 'data/dataset/processed/e_test'

    def processed(self, parts=None, extra=True):
        io = self.IO()
        if parts is None:
            parts = ['train', 'val', 'test']

        x_train = io.read_pq(io.abs_path(self.X_TRAIN_PATH))
        y_train = io.read_pq(io.abs_path(self.Y_TRAIN_PATH))
        e_train = io.read_pq(io.abs_path(self.E_TRAIN_PATH))

        x_val = io.read_pq(io.abs_path(self.X_VAL_PATH))
        y_val = io.read_pq(io.abs_path(self.Y_VAL_PATH))
        e_val = io.read_pq(io.abs_path(self.E_VAL_PATH))

        x_test = io.read_pq(io.abs_path(self.X_TEST_PATH))
        y_test = io.read_pq(io.abs_path(self.Y_TEST_PATH))
        e_test = io.read_pq(io.abs_path(self.E_TEST_PATH))

        data = {}

        if 'train' in parts:
            data['x_train'] = x_train
            data['y_train'] = y_train
            if extra:
                data['e_train'] = e_train
        if 'val' in parts:
            data['x_val'] = x_val
            data['y_val'] = y_val
            if extra:
                data['e_val'] = e_val
        if 'test' in parts:
            data['x_test'] = x_test
            data['y_test'] = y_test
            if extra:
                data['e_test'] = e_test
        return data

    def metadata_synch(self):
        io = self.IO()
        print('\nUpdating metadata in sitemap.parquet...')
        # Define file path
        sitemap_path = io.abs_path(self.SITEMAP_PATH)
        pages_path = io.abs_path(self.PAGES_PATH)
        content_path = io.abs_path(self.CONTENT_PATH)
        # Check if file exists
        sitemap_exists = io.exists(sitemap_path)
        page_exists = io.exists(pages_path)
        content_exists = io.exists(content_path)
        # Break if sitemap dies not exist
        if not sitemap_exists:
            print('\nSitemap *.parquet does not exist.')
            return
        # Hide warnings
        self.pd.options.mode.chained_assignment = None
        # Extract into pandas data frame
        sitemap = updated = io.read_pq(sitemap_path)
        pages = io.read_pq(pages_path) if page_exists else None
        content = io.read_pq(content_path) if content_exists else None
        # Synchronise sitemap metadata with extracted and parsed data frames
        bar = self.tqdm(total=len(sitemap), bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        for index in sitemap.index:
            url = sitemap.url[index]
            is_extracted = (pages['url'] == url).any() if pages is not None else False
            is_parsed = (content['url'] == url).any() if content is not None else False
            # Update extract status
            if is_extracted:
                sitemap['extract'][index] = True
                sitemap['extract_ts'][index] = io.now()
            else:
                sitemap['extract'][index] = False
                sitemap['extract_ts'][index] = None
            # Update parse status
            if is_parsed:
                sitemap['parse'][index] = True
                sitemap['parse_ts'][index] = io.now()
            else:
                sitemap['parse'][index] = False
                sitemap['parse_ts'][index] = None
            bar.update(1)
        bar.close()
        io.pause(1)
        # Update sitemap
        updated.update(sitemap)
        io.save_pq(data=updated, path=sitemap_path)
        print('Metadata in sitemap.parquet are up to date.')

    def save_as_parquet(self, data, path, dedup_columns):
        io = self.IO()
        directory, file_name = io.dir_and_base(path)
        print('\nSaving data into *.parquet...')
        # If file exists but has different schema (columns) – remove it
        if io.exists(path):
            existing = io.read_pq(path)
            identical_columns = len(existing.columns.intersection(data.columns)) == data.shape[1]
            if not identical_columns:
                print('Datasets have different schemas. Removing the old version.')
                io.remove(path)
        # If exists – update
        if io.exists(path):
            existing = io.read_pq(path)
            updated = self.pd.concat([existing, data]).drop_duplicates(subset=dedup_columns).reset_index(drop=True)
            io.save_pq(data=updated, path=path)
            # Log and communicate
            total, existing = len(updated.index), len(existing.index)
            new = total - existing
            duplicates = len(data) - new
            self.logger(directory, file_name, total, new, existing)
            print(f'Adding {new} new rows. {duplicates} duplicates excluded. {total} rows in total.')
        # Else – create a new file
        else:
            data = data.drop_duplicates(subset=dedup_columns).reset_index(drop=True)
            io.save_pq(data=data, path=path)
            new = total = len(data.index)
            self.logger(directory, file_name, total, new)
            print(f'Creating {len(data.index)} new rows.')

    def remove_duplicates(self, original_dataset_path, target_dataset_path, columns, dedup_columns):
        io = self.IO()
        entities_exists = io.exists(target_dataset_path)
        if entities_exists:
            if columns:
                new = io.read_pq(original_dataset_path)[columns]
                old = io.read_pq(target_dataset_path)[columns]
            else:
                new = io.read_pq(original_dataset_path)
                old = io.read_pq(target_dataset_path)
            data = self.pd.concat([old, new]) \
                .drop_duplicates(subset=dedup_columns, keep=False) \
                .reset_index(drop=True)
            print(f'Dataset exists and contains {len(old)} rows. Adding {len(data)} new rows.')
        else:
            if columns:
                data = io.read_pq(original_dataset_path)[columns]
            else:
                data = io.read_pq(original_dataset_path)[columns]
            print(f'Dataset does not exists yet. Extracting {len(data)} new rows.')
        return data

    def logger(self, directory, file_name, total, new, existing=0):
        io = self.IO()
        logger_path = io.abs_path(directory, 'log.csv')
        io.make_dir(directory)
        ts = io.now()
        if io.exists(logger_path):
            with open(logger_path, 'a') as f:
                writer = self.csv.writer(f)
                writer.writerow([ts, file_name, total, new, existing])
        else:
            header = ['ts', 'file_name', 'total', 'new', 'existing']
            with open(logger_path, 'w') as f:
                writer = self.csv.writer(f)
                writer.writerow(header)
                writer.writerow([ts, file_name, total, new, existing])

    def errors(self, directory, index, url, error):
        io = self.IO()
        errors_path = io.abs_path(directory, 'errors.csv')
        io.make_dir(directory)
        ts = io.now()
        if io.exists(errors_path):
            with open(errors_path, 'a') as f:
                writer = self.csv.writer(f)
                writer.writerow([ts, index, url, error])
        else:
            header = ['ts', 'index', 'url', 'error']
            with open(errors_path, 'w') as f:
                writer = self.csv.writer(f)
                writer.writerow(header)
                writer.writerow([ts, index, url, error])

