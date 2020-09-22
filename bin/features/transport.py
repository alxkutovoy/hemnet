class Transport(object):

    import functools
    import pandas as pd

    from tqdm import tqdm

    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO
    from utils.geo import Geo

    def transport(self):
        print('\nEnrich dataset with Stockholm public transportation features:')
        helper, io = self.Helper(), self.IO()
        # Get raw property data
        data = helper.remove_duplicates(self.File.SUBSET, self.File.TRANSPORT, ['url', 'coordinates'], ['url'])
        # Check if anything to work on
        if len(data) == 0:
            print('There are no new properties to work on.')
            return
        # Get transport data
        transport = io.read_pq(self.File.SL_PROCESSED)
        transport_types = self._get_transport_types(transport)  # A list of transportation types
        # Enrich with transportation data
        distances = [[0, 250], [0, 500], [0, 1000]]
        io.pause(2)
        bar = self.tqdm(total=len(data), bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        io.pause()
        for index in data.index:
            entry = data.loc[[index]]
            coordinates = entry.coordinates.item()
            # Calculate distances from a specific given location to all public transport spots
            self.pd.set_option('mode.chained_assignment', None)
            transport['distance'] = self._calculate_distances(transport, coordinates)
            for category in transport_types:
                query, suffix = self._get_filter(category), self._get_suffix(category)
                subset = transport.query(query)
                self._column_check(data, 'nearest_stop', suffix)
                data['nearest_stop' + suffix][index] = min(subset.distance)
                for distance in distances:
                    self._generate_distance_features(index, data, distance, subset, category)
            bar.update(1)
        bar.close()
        io.pause()  # Prevents issues with the layout of update messages im terminal
        # Save
        helper.update_pq(data=data, path=self.File.TRANSPORT, dedup=['url'])
        print("\nCompleted.")

    def _generate_distance_features(self, index, data, distance, transport, category):
        min_dist, max_dist = distance[0], distance[1]
        suffix = self._get_suffix(category, min_dist, max_dist)
        query_filter = self._get_filter(category, min_dist, max_dist)
        for name in ['lines', 'stops', 'points']:
            self._column_check(data, name, suffix)
        subset = transport.query(query_filter)
        data['lines' + suffix][index] = self._count_distinct_lines(subset)
        data['stops' + suffix][index] = len(list(subset.StopAreaNumber.unique()))
        data['points' + suffix][index] = len(list(subset.StopPointNumber.unique()))

    def _calculate_distances(self, transport, coordinates):
        geo = self.Geo()
        return transport.apply(lambda x: geo.gcs_to_dist(
            coordinates, [x.LocationNorthingCoordinate, x.LocationEastingCoordinate]), axis=1)

    def _get_transport_types(self, t):
        transport_types = ['ALL'] + list(t.StopAreaTypeCode.unique())
        if 'UNKNOWN' in transport_types:
            transport_types.remove('UNKNOWN')
        return transport_types

    def _get_suffix(self, category, min_dist=None, max_dist=None):
        if min_dist is not None and max_dist is not None:
            if category == 'ALL':
                return '_' + str(min_dist) + '_' + str(max_dist)
            else:
                return '_' + category.lower() + '_' + str(min_dist) + '_' + str(max_dist)
        else:
            return '' if category == 'ALL' else '_' + category.lower()

    def _get_filter(self, category, min_dist=None, max_dist=None):
        if category == 'ALL':
            if min_dist is not None and max_dist is not None:
                return f'distance >= {min_dist} & distance < {max_dist}'
            else:
                return 'StopAreaTypeCode != ""'
        else:
            if min_dist is not None and max_dist is not None:
                return f'distance >= {min_dist} & distance < {max_dist} & StopAreaTypeCode == "{category}"'
            else:
                return f'StopAreaTypeCode == "{category}"'

    def _count_distinct_lines(self, subset):
        if list(subset.lines):
            flattened_list = self.functools.reduce(lambda x, y: x + y, list(subset.lines))
            deduplicated_list = list(set(flattened_list))
            return len(deduplicated_list)
        else:
            return 0

    def _column_check(self, data, name, suffix):
        if name + suffix not in data.columns:
            data[name + suffix] = None


if __name__ == '__main__':
    Transport().transport()
