class Enrichment(object):

    import pandas as pd

    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO

    def data(self):
        print('\nCompile enriched dataset:')
        helper, io = self.Helper(), self.IO()
        # Raw data
        data = io.read_pq(self.File.SUBSET)
        # Destinations
        destinations = io.read_pq(self.File.DESTINATIONS)
        destinations = destinations.drop(['latitude', 'longitude'], axis=1)
        # Entities
        entities = io.read_pq(self.File.ENTITIES)
        entities = entities.drop(['latitude', 'longitude'], axis=1)
        # Addresses
        addresses = io.read_pq(self.File.ADDRESSES)
        addresses = addresses.drop(['gmaps_query', 'gmaps_raw_response'], axis=1)
        # Transport
        transport = io.read_pq(self.File.TRANSPORT)
        transport = transport.drop(['latitude', 'longitude'], axis=1)
        # Clusters
        clusters = io.read_pq(self.File.CLUSTERS)
        clusters = clusters.drop(['latitude', 'longitude'], axis=1)
        # Join
        data = self.pd.merge(data, destinations, how='left', on=['url'])    # Add destinations
        data = self.pd.merge(data, entities, how='left', on=['url'])        # Add entities
        data = self.pd.merge(data, addresses, how='left', on=['url'])       # Add addresses
        data = self.pd.merge(data, transport, how='left', on=['url'])       # Add transport
        data = self.pd.merge(data, clusters, how='left', on=['url'])        # Add transport
        # Save
        if len(data) == len(destinations) == len(entities) == len(transport) == len(addresses):
            io.save_pq(data=data, path=self.File.ENRICHED_SUBSET)
            print("\nCompleted.")
        else:
            print(f'\nSorry, something went wrong. Lengths of enriched datasets are not matching:'
                  f'\nSubset: {len(data)}. Destinations: {len(destinations)}. Entities: {len(entities)}.'
                  f'Transport: {len(transport)}. Addresses: {len(addresses)}.')


if __name__ == '__main__':
    Enrichment().data()
