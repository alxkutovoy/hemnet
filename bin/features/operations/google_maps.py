class GoogleMaps(object):

    import json
    import os
    import pandas as pd
    import requests

    from datetime import datetime
    from os import path
    from tqdm import tqdm

    from bin.helpers.helper import Helper

    def get_unprocessed_data(self):
        print('\nExtract Google Maps API data:')
        helper = self.Helper()
        # List of items
        print("\nExtracting...")
        path_epicenters, path_entities = "../../../resource/entities.json", "../../../resource/epicenters.json"
        epicenters, entities = self._json_to_df(path_epicenters), self._json_to_df(path_entities)
        data = self._cartesian_product_basic(entities, epicenters)
        # Check if exists and remove already processed items
        directory, name = '../../../data/library/gmaps',  'unprocessed.parquet'
        path = self.path.join(directory, name)
        exists = self.os.path.isfile(path)
        dedup_columns = ['entity_id', 'epicenter_id']
        if exists:
            print('Excluded already extracted data from a new job.')
            data = self._deduplicate_items(data, path, dedup_columns)
        # Check if anything to work on
        if len(data) == 0:
            print('\nThere are no new entities to work on.')
            return
        # Add dummy columns
        data.insert(0, 'ts', None)
        data['request'], data['pages'], data['payload'] = None, None, None
        # Get data from Google
        helper.pause(2)
        bar = self.tqdm(total=len(data), bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        helper.pause()
        for index in data.index:
            entry = data.loc[index]
            request = self._request_constructor(latitude=entry.epicenter_lat,
                                                longitude=entry.epicenter_lon,
                                                radius=entry.epicenter_radius,
                                                query=entry.entity_query,
                                                entity_type=entry.entity_type,
                                                rankby=entry.entity_rankby)
            payload = self.requests.get(request).json()
            results = payload['results']
            # Check if response is split into several pages and process all of them if that's the case
            pages = 1
            while 'next_page_token' in payload:
                next_page_token = payload['next_page_token']
                request = self._request_constructor(token=next_page_token)
                # "There is a short delay between when a token is issued, and when it will become valid." (c) Google
                helper.pause(2)
                payload = self.requests.get(request).json()
                results += payload['results']
                pages += 1
            self.pd.set_option('mode.chained_assignment', None)
            data['ts'][index] = self.datetime.now().replace(microsecond=0)
            data['payload'][index] = self.json.dumps(results)
            data['pages'][index] = pages
            data['request'][index] = request
            bar.update(1)
        bar.close()
        helper.pause()  # Prevents issues with the layout of update messages im terminal
        helper.save_as_parquet(data, directory, name, dedup_columns)
        print("\nCompleted.")

    def _request_constructor(self,
                             query=None,
                             latitude=None,
                             longitude=None,
                             rankby=None,
                             radius=None,
                             entity_type=None,
                             language='en',
                             token=None):
        helper = self.Helper()
        api_key = 'key=' + helper.get_api_key('google', 'maps_key')
        body = 'https://maps.googleapis.com/maps/api/place/textsearch/json?'
        if token:
            pagetoken = 'pagetoken=' + token
            parameters = '&'.join([pagetoken, api_key])
        else:
            query = 'query=' + query if radius else ''
            location = 'location=' + str(latitude) + ',' + str(longitude)
            radius = 'radius=' + str(radius) if radius and not rankby else ''
            entity_type = 'type=' + entity_type if entity_type else ''
            language = 'language=' + language
            rankby = 'rankby=' + rankby if rankby else ''
            # Construct parameters
            elements = [query, location, rankby, radius, entity_type, language, api_key]
            parameters = '&'.join(list(filter(None, elements)))
        return body + parameters

    def _deduplicate_items(self, data, path, dedup_columns):
        old = self.pd.read_parquet(path)
        old = old.drop(['ts', 'request', 'pages', 'payload'], axis=1)
        data = self.pd.concat([old, data])\
            .drop_duplicates(subset=dedup_columns, keep=False)\
            .reset_index(drop=True)
        return data

    def _json_to_df(self, path):
        with open(path) as json_file:
            data = self.json.load(json_file)
        return self.pd.DataFrame(data)

    def _cartesian_product_basic(self, left, right):
        return left.assign(key=1).merge(right.assign(key=1), on='key').drop('key', 1)


if __name__ == '__main__':
    GoogleMaps().get_unprocessed_data()
