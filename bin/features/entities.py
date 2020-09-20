class Entities(object):

    import pandas as pd

    from os import path
    from pathlib import Path
    from tqdm import tqdm

    from bin.helpers.helper import Helper
    from utils.files import Utils

    def entities(self):
        print('\nEnrich dataset with Google Maps entities features:')
        helper, utils = self.Helper(), self.Utils()
        # Get raw property data
        dataset_path = utils.get_full_path('data/dataset/raw/data.parquet')
        entities_directory, entities_name = utils.get_full_path('data/dataset/features'), 'entities.parquet'
        entities_path = self.path.join(entities_directory, entities_name)
        columns, dedup_columns = ['url', 'coordinates'], ['url']
        data = helper.remove_duplicates(dataset_path, entities_path, columns, dedup_columns)
        # Check if anything to work on
        if len(data) == 0:
            print('There are no new properties to work on.')
            return
        # Get Google Maps data
        gmaps_path = utils.get_full_path('data/library/gmaps/processed.parquet')
        gmaps = self.pd.read_parquet(gmaps_path, engine="fastparquet")
        entities_types = list(gmaps.entity_category.unique())  # A list of entity types
        # Enrich with entities data
        print('\nCalculating features...')
        distances = [[0, 250], [0, 500], [0, 1000]]
        helper.pause(2)
        bar = self.tqdm(total=len(data), bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        helper.pause()
        for index in data.index:
            entry = data.loc[[index]]
            coordinates = entry.coordinates.item()
            # Calculate distances from a specific given location to all entities
            self.pd.set_option('mode.chained_assignment', None)
            gmaps['distance'] = gmaps.apply(lambda x: helper.gcs_to_dist(
                coordinates, [x.place_lat, x.place_lon]), axis=1)
            for category in entities_types:
                query, suffix = self._get_filter(category), self._get_suffix(category)
                subset = gmaps.query(query)
                self._column_check(data, 'nearest', suffix)
                data['nearest' + suffix][index] = min(subset.distance)
                for distance in distances:
                    self._generate_distance_features(index, data, distance, subset, category)
            bar.update(1)
        bar.close()
        helper.pause()  # Prevents issues with the layout of update messages im terminal
        # Save
        self.Path(entities_directory).mkdir(parents=True, exist_ok=True)
        helper.save_as_parquet(data, entities_directory, entities_name, ['url'])
        print("\nCompleted.")

    def _generate_distance_features(self, index, data, distance, gmaps, category):
        min_dist, max_dist = distance[0], distance[1]
        suffix = self._get_suffix(category, min_dist, max_dist)
        query_filter = self._get_filter(category, min_dist, max_dist)
        for name in ['entities', 'scores_mean', 'scores_median', 'reviews_mean', 'reviews_median']:
            self._column_check(data, name, suffix)
        subset = gmaps.query(query_filter)
        data['entities' + suffix][index] = len(list(subset.place_id.unique()))
        data['scores_mean' + suffix][index] = round(subset.rating.mean(), 2)
        data['scores_median' + suffix][index] = round(subset.rating.median(), 2)
        data['reviews_mean' + suffix][index] = round(subset.user_ratings_total.mean(), 2)
        data['reviews_median' + suffix][index] = round(subset.user_ratings_total.median(), 2)

    def _get_suffix(self, category, min_dist=None, max_dist=None):
        if min_dist is not None and max_dist is not None:
            return '_' + category.lower() + '_' + str(min_dist) + '_' + str(max_dist)
        else:
            return '_' + category.lower()

    def _get_filter(self, category, min_dist=None, max_dist=None):
        if min_dist is not None and max_dist is not None:
            return f'distance >= {min_dist} & distance < {max_dist} & entity_category == "{category}"'
        else:
            return f'entity_category == "{category}"'

    def _column_check(self, data, name, suffix):
        if name + suffix not in data.columns:
            data[name + suffix] = None


if __name__ == '__main__':
    Entities().entities()
