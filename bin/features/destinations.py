class Destinations(object):

    import json
    import pandas as pd

    from os import path
    from pathlib import Path

    from bin.helpers.helper import Helper
    from utils.files import Utils

    def destinations(self):
        print('\nEnrich dataset with destinations to strategic points features:')
        helper, utils = self.Helper(), self.Utils()
        # Get raw property data
        dataset_path = utils.get_full_path('data/dataset/raw/data.parquet')
        destinations_directory, destinations_name = utils.get_full_path('data/dataset/features'), 'destinations.parquet'
        destinations_path = self.path.join(destinations_directory, destinations_name)
        columns, dedup_columns = ['url', 'coordinates'], ['url']
        data = helper.remove_duplicates(dataset_path, destinations_path, columns, dedup_columns)
        # Check if anything to work on
        if len(data) == 0:
            print('There are no new properties to work on.')
            return
        # List of items
        print("\nExtracting...")
        points_path = utils.get_full_path("resource/points.json")
        with open(points_path) as json_file:
            points = self.json.load(json_file)
        # Calculate distances from a specific given location to all properties
        self.pd.set_option('mode.chained_assignment', None)
        for point in points:
            column_name = 'distance_to_' + point['point_name']
            point_coordinates = [point['point_lat'], point['point_lng']]
            data[column_name] = data.apply(lambda x: helper.gcs_to_dist(x.coordinates, point_coordinates), axis=1)
        # Save
        self.Path(destinations_directory).mkdir(parents=True, exist_ok=True)
        helper.save_as_parquet(data, destinations_directory, destinations_name, dedup_columns)
        print("\nCompleted.")


if __name__ == '__main__':
    Destinations().destinations()
