class Destinations(object):
    import json
    import pandas as pd

    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO
    from utils.geo import Geo

    def destinations(self):
        print('\nEnrich dataset with destinations to strategic points features:')
        helper, io = self.Helper(), self.IO()
        # Get raw property data
        data = helper.remove_duplicates(original_path=self.File.SUBSET,
                                        target_path=self.File.DESTINATIONS,
                                        select=['url', 'latitude', 'longitude'],
                                        dedup=['url'])
        # Check if anything to work on
        if len(data) == 0:
            print('There are no new properties to work on.')
            return
        # Calculate distances from a specific given location to all properties
        data = self._distances_calculator(data)
        # Save
        helper.update_pq(data=data, path=self.File.DESTINATIONS, dedup=['url'])
        print("\nCompleted.")

    def request_destinations(self, request):
        original_columns = list(request.columns)
        output = self._distances_calculator(request)
        return output.drop(columns=original_columns, axis=1)

    def _distances_calculator(self, data):
        with open(self.File.POINTS) as json_file:
            points = self.json.load(json_file)
        self.pd.set_option('mode.chained_assignment', None)
        for point in points:
            column_name = 'distance_to_' + point['point_name']
            point_coordinates = [point['point_lat'], point['point_lng']]
            data[column_name] = data.apply(lambda x: self.Geo().gcs_to_dist(
                point_a=[x.latitude, x.longitude], point_b=point_coordinates), axis=1)
        return data


if __name__ == '__main__':
    Destinations().destinations()
