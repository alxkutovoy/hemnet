class Helper(object):

    import csv
    import pandas as pd

    from tqdm import tqdm

    from utils.var import File, Dir
    from utils.io import IO

    def metadata_synch(self):
        io = self.IO()
        print('\nUpdating metadata in sitemap.parquet...')
        # Break if sitemap dies not exist
        if not io.exists(self.File.SITEMAP):
            print('\nSitemap *.parquet does not exist.')
            return
        # Hide warnings
        self.pd.options.mode.chained_assignment = None
        # Extract into pandas data frame
        sitemap = updated = io.read_pq(self.File.SITEMAP)
        pages = io.squash_pq(self.Dir.PAGES) \
            if io.file_counter(directory=self.Dir.PAGES, pattern='*.parquet') else None
        content = io.read_pq(self.File.CONTENT) if io.exists(self.File.CONTENT) else None
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
        io.save_pq(data=updated, path=self.File.SITEMAP)
        print('Metadata in sitemap.parquet are up to date.')

    def update_pq(self, data, path, dedup, com=True):
        io = self.IO()
        directory, file_name = io.dir_and_base(path)
        io.make_dir(directory)
        print('\nSaving data into *.parquet...') if com else None
        # If file exists but has different schema (columns) – remove it
        if io.exists(path):
            existing = io.read_pq(path)
            identical_columns = len(existing.columns.intersection(data.columns)) == data.shape[1]
            if not identical_columns:
                print('Datasets have different schemas. Removing the old version.') if com else None
                io.remove(path)
        # If exists – update
        if io.exists(path):
            existing = io.read_pq(path)
            updated = self.pd.concat([existing, data]).drop_duplicates(subset=dedup).reset_index(drop=True)
            io.save_pq(data=updated, path=path)
            # Log and communicate
            total, existing = len(updated.index), len(existing.index)
            new = total - existing
            duplicates = len(data) - new
            self.logger(directory, file_name, total, new, existing)
            print(f'Adding {new} new rows. {duplicates} duplicates excluded. {total} rows in total.') if com else None
        # Else – create a new file
        else:
            data = data.drop_duplicates(subset=dedup).reset_index(drop=True)
            io.save_pq(data=data, path=path)
            new = total = len(data.index)
            self.logger(directory, file_name, total, new)
            print(f'Creating {len(data.index)} new rows.') if com else None

    def remove_duplicates(self, original_path, target_path, select, dedup):
        io = self.IO()
        exists = io.exists(target_path)
        if exists:
            new = io.read_pq(original_path)[select] if select else io.read_pq(original_path)
            old = io.read_pq(target_path)[dedup]
            data = self.pd.concat([old, new]) \
                .drop_duplicates(subset=dedup, keep=False) \
                .reset_index(drop=True)
            print(f'Dataset exists and contains {len(old)} rows. Adding {len(data)} new rows.')
        else:
            if select:
                data = io.read_pq(original_path)[select]
            else:
                data = io.read_pq(original_path)[select]
            print(f'Dataset does not exists yet. Extracting {len(data)} new rows.')
        return data

    def logger(self, directory, file_name, total, new, existing=0):
        io = self.IO()
        logger_path = io.path_join(directory, 'log.csv')
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
        errors_path = io.path_join(directory, 'errors.csv')
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
