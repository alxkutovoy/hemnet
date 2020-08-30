class Enrichment(object):

    import pandas as pd

    from pathlib import Path

    from bin.helpers.helper import Helper
    from utils.files import Utils

    def data(self):
        print('\nGenerate a sample from the dataset:')
        helper, utils = self.Helper(), self.Utils()
        # Raw data
        data_path = utils.get_full_path('data/dataset/raw/data.parquet')
        data = self.pd.read_parquet(data_path, engine="fastparquet")
        # Destinations
        destinations_path = utils.get_full_path('data/dataset/features/destinations.parquet')
        destinations = self.pd.read_parquet(destinations_path, engine="fastparquet")
        destinations = destinations.drop(['coordinates'], axis=1)
        # Entities
        entities_path = utils.get_full_path('data/dataset/features/entities.parquet')
        entities = self.pd.read_parquet(entities_path, engine="fastparquet")
        entities = entities.drop(['coordinates'], axis=1)
        # Transport
        transport_path = utils.get_full_path('data/dataset/features/transport.parquet')
        transport = self.pd.read_parquet(transport_path, engine="fastparquet")
        transport = transport.drop(['coordinates'], axis=1)
        # Join
        data = self.pd.merge(data, destinations, how='left', on=['url'])    # Add destinations
        data = self.pd.merge(data, entities, how='left', on=['url'])        # Add entities
        data = self.pd.merge(data, transport, how='left', on=['url'])       # Add transport
        # Save
        directory, name = utils.get_full_path('data/dataset/enriched'), 'data.parquet'
        self.Path(directory).mkdir(parents=True, exist_ok=True)
        helper.save_as_parquet(data, directory, name, ['url'])
        print("\nCompleted.")


if __name__ == '__main__':
    Enrichment().data()
