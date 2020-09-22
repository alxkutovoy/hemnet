class Destinations(object):

    import json
    import pandas as pd

    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO
    from utils.geo import Geo

    def destinations(self):
        print('\nEnrich dataset with destinations to strategic points features:')
        helper, io, geo = self.Helper(), self.IO(), self.Geo()
        # Get raw property data
        data = helper.remove_duplicates(self.File.SUBSET, self.File.DESTINATIONS, ['url', 'coordinates'], ['url'])
        # Check if anything to work on
        if len(data) == 0:
            print('There are no new properties to work on.')
            return
        # List of items
        print("\nExtracting...")
        with open(self.File.POINTS) as json_file:
            points = self.json.load(json_file)
        # Calculate distances from a specific given location to all properties
        self.pd.set_option('mode.chained_assignment', None)
        for point in points:
            column_name = 'distance_to_' + point['point_name']
            point_coordinates = [point['point_lat'], point['point_lng']]
            data[column_name] = data.apply(lambda x: geo.gcs_to_dist(x.coordinates, point_coordinates), axis=1)
        # Save
        helper.update_pq(data=data, path=self.File.DESTINATIONS, dedup=['url'])
        print("\nCompleted.")


if __name__ == '__main__':
    Destinations().destinations()
