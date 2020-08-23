class Helper:

    import csv
    import json
    import os
    import pandas as pd
    import random
    import time

    from datetime import datetime
    from os import path
    from pathlib import Path
    from tqdm import tqdm

    def pause(self, sec=random.randint(3, 7) / 10):
        return self.time.sleep(sec)

    def file_counter(self, directory):
        return sum([len(files) for r, d, files in self.os.walk(self.path.join(self.os.getcwd(), directory))])

    def logger(self, directory, total, new, existing=0):
        logger_path = self.path.join(directory, 'log.csv')
        self.Path(directory).mkdir(parents=True, exist_ok=True)
        ts = self.datetime.now().replace(microsecond=0)
        if self.os.path.isfile(logger_path):
            with open(logger_path, 'a') as f:
                writer = self.csv.writer(f)
                writer.writerow([ts, total, new, existing])
        else:
            header = ['ts', 'total', 'new', 'existing']
            with open(logger_path, 'w') as f:
                writer = self.csv.writer(f)
                writer.writerow(header)
                writer.writerow([ts, total, new, existing])

    def errors(self, directory, index, url, error):
        errors_path = self.path.join(directory, 'errors.csv')
        self.Path(directory).mkdir(parents=True, exist_ok=True)
        ts = self.datetime.now().replace(microsecond=0)
        if self.os.path.isfile(errors_path):
            with open(errors_path, 'a') as f:
                writer = self.csv.writer(f)
                writer.writerow([ts, index, url, error])
        else:
            header = ['ts', 'index', 'url', 'error']
            with open(errors_path, 'w') as f:
                writer = self.csv.writer(f)
                writer.writerow(header)
                writer.writerow([ts, index, url, error])

    def save_as_parquet(self, data, directory, file_name, dedup_column):
        file_path = self.path.join(directory, file_name)
        print('\nSaving data into *.parquet...')
        if self.os.path.isfile(file_path):
            existing = self.pd.read_parquet(file_path)
            updated = self.pd.concat([existing, data]).drop_duplicates(subset=[dedup_column]).reset_index(drop=True)
            updated.to_parquet(file_path, compression='gzip')
            # Log and communicate
            total, existing = len(updated.index), len(existing.index)
            new = total - existing
            duplicates = len(data) - new
            self.logger(directory, total, new, existing)
            print('Adding', new, 'new rows.', duplicates, 'duplicates excluded.', total, 'rows in total.')
        else:
            self.Path(directory).mkdir(parents=True, exist_ok=True)
            data = data.drop_duplicates(subset=[dedup_column]).reset_index(drop=True)
            data.to_parquet(file_path, compression='gzip')
            new = total = len(data.index)
            self.logger(directory, total, new)
            print('Creating', len(data.index), 'new rows.')

    def metadata_synch(self):
        print('\nUpdating metadata in sitemap.parquet...')
        # Define file path
        sitemap_path = '../../data/sitemap/sitemap.parquet'
        pages_path = '../../data/pages/pages.parquet'
        content_path = '../../data/content/content.parquet'
        # Check if file exists
        sitemap_exists = self.os.path.isfile(sitemap_path)
        page_exists = self.os.path.isfile(pages_path)
        content_exists = self.os.path.isfile(content_path)
        # Break if sitemap dies not exist
        if not sitemap_exists:
            print('\nSitemap *.parquet does not exist.')
            return
        # Hide warnings
        self.pd.options.mode.chained_assignment = None
        # Extract into pandas data frame
        sitemap = updated = self.pd.read_parquet(sitemap_path)
        pages = self.pd.read_parquet(pages_path) if page_exists else None
        content = self.pd.read_parquet(content_path) if content_exists else None
        # Synchronise sitemap metadata with extracted and parsed data frames
        bar = self.tqdm(total=len(sitemap), bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        for index in sitemap.index:
            url = sitemap.url[index]
            is_extracted = (pages['url'] == url).any() if pages is not None else False
            is_parsed = (content['url'] == url).any() if content is not None else False
            # Update extract status
            if is_extracted:
                sitemap['extract'][index] = True
                sitemap['extract_ts'][index] = self.datetime.now().replace(microsecond=0)
            else:
                sitemap['extract'][index] = False
                sitemap['extract_ts'][index] = None
            # Update parse status
            if is_parsed:
                sitemap['parse'][index] = True
                sitemap['parse_ts'][index] = self.datetime.now().replace(microsecond=0)
            else:
                sitemap['parse'][index] = False
                sitemap['parse_ts'][index] = None
            bar.update(1)
        bar.close()
        self.pause()
        # Update sitemap
        updated.update(sitemap)
        updated.to_parquet(sitemap_path, compression='gzip')
        print('Metadata in sitemap.parquet are up to date.')

    def get_api_key(self, category, api):
        try:
            path = '../../../secrets/api_keys.json'
            with open(path) as json_file:
                api_keys = self.json.load(json_file)
                return api_keys[category][api]
        except Exception as e:
            print('There is no such API key in your list of secrets.')
            return None
