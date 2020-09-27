class Entities(object):

    import pandas as pd

    from tqdm import tqdm

    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO
    from utils.geo import Geo

    def entities(self):
        print('\nEnrich dataset with Google Maps entities features:')
        helper, io, geo = self.Helper(), self.IO(), self.Geo()
        # Get raw property data
        data = helper.remove_duplicates(original_path=self.File.SUBSET,
                                        target_path=self.File.ENTITIES,
                                        select=['url', 'latitude', 'longitude'],
                                        dedup=['url'])
        # Check if anything to work on
        if len(data) == 0:
            print('There are no new properties to work on.')
            return
        # Get Google Maps data
        gmaps = io.read_pq(self.File.ENTITIES_PROCESSED)
        entities_types = list(gmaps.entity_category.unique())  # A list of entity types
        # Enrich with entities data
        print('\nCalculating features...')
        distances = [[0, 250], [0, 500], [0, 1000]]
        io.pause(2)
        bar = self.tqdm(total=len(data), bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        io.pause()
        for index in data.index:  # TODO: Refactor and abstract in a smarter way
            entry = data.loc[[index]]
            coordinates = [entry.latitude.item(), entry.longitude.item()]
            # Calculate distances from a specific given location to all entities
            self.pd.set_option('mode.chained_assignment', None)
            gmaps['distance'] = gmaps.apply(lambda x: geo.gcs_to_dist(
                coordinates, [x.place_lat, x.place_lon]), axis=1)
            for category in entities_types:
                query, suffix = self._get_filter(category), self._get_suffix(category)
                subset = gmaps.query(query)
                data = self._column_check(data, 'nearest', suffix)
                data['nearest' + suffix][index] = min(subset.distance)
                for distance in distances:
                    data = self._generate_distance_features(index=index, data=data, distance=distance,
                                                            gmaps=subset, category=category)
            bar.update(1)
        bar.close()
        io.pause()  # Prevents issues with the layout of update messages im terminal
        # Save
        helper.update_pq(data=data, path=self.File.ENTITIES, dedup=['url'])
        print("\nCompleted.")

    def request_entities(self, request):
        io, geo = self.IO(), self.Geo()
        original_columns = list(request.columns)
        gmaps = io.read_pq(self.File.ENTITIES_PROCESSED)
        entities_types = list(gmaps.entity_category.unique())  # A list of entity types
        distances = [[0, 250], [0, 500], [0, 1000]]
        coordinates = [request.latitude.item(), request.longitude.item()]
        self.pd.set_option('mode.chained_assignment', None)
        gmaps['distance'] = gmaps.apply(lambda x: geo.gcs_to_dist(
            coordinates, [x.place_lat, x.place_lon]), axis=1)
        for category in entities_types:
            query, suffix = self._get_filter(category), self._get_suffix(category)
            subset = gmaps.query(query)
            request = self._column_check(request, 'nearest', suffix)
            request['nearest' + suffix] = min(subset.distance)
            for distance in distances:
                request = self._generate_distance_features(data=request, distance=distance, gmaps=subset,
                                                           category=category)
        return request.drop(columns=original_columns, axis=1)

    def _generate_distance_features(self, data, distance, gmaps, category, index=0):
        min_dist, max_dist = distance[0], distance[1]
        suffix = self._get_suffix(category, min_dist, max_dist)
        query_filter = self._get_filter(category, min_dist, max_dist)
        for name in ['entities', 'scores_mean', 'scores_median', 'reviews_mean', 'reviews_median']:
            data = self._column_check(data, name, suffix)
        subset = gmaps.query(query_filter)
        data['entities' + suffix][index] = len(list(subset.place_id.unique()))
        data['scores_mean' + suffix][index] = round(subset.rating.mean(), 2)
        data['scores_median' + suffix][index] = round(subset.rating.median(), 2)
        data['reviews_mean' + suffix][index] = round(subset.user_ratings_total.mean(), 2)
        data['reviews_median' + suffix][index] = round(subset.user_ratings_total.median(), 2)
        return data

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
        return data


if __name__ == '__main__':
    Entities().entities()
