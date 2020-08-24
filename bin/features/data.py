class Data(object):
    import pandas as pd

    from pathlib import Path

    from bin.helpers.helper import Helper

    def generate_dataset(self):
        print('\nGenerate a sample from the dataset:')
        helper = self.Helper()
        content_path = '../../data/content/content.parquet'
        directory, name = '../../data/dataset/raw', 'data.parquet'
        content = self.pd.read_parquet(content_path)
        # Filter relevant entries
        print('\nSample relevant entries...')
        data = content.query('urban_area == "Stockholm tätort" & property_type == "Lägenhet"')  # Configurable filters
        # Save
        self.Path(directory).mkdir(parents=True, exist_ok=True)
        helper.save_as_parquet(data, directory, name, 'url')
        print("\nCompleted.")


if __name__ == '__main__':
    Data().generate_dataset()
