class GoogleMapsEntities(object):

    import json
    import pandas as pd
    import requests

    from tqdm import tqdm

    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO
    from utils.api import API

    def get_processed_data(self, pull=True):
        print('\nProcessing Google Maps API data:')
        helper, io = self.Helper(), self.IO()
        # Check if unprocessed dataset exists
        if pull or not io.exists(self.File.ENTITIES_UNPROCESSED):
            print('Attempt extracting Google Maps dataset...')
            self.get_unprocessed_data()
        # Process data
        print('\nProcess raw dataset...')
        data = io.read_pq(self.File.ENTITIES_UNPROCESSED)
        # Convert payload from str into json and explode it (1 row = 1 entity)
        data['payload'] = data['payload'].apply(lambda x: self.json.loads(x))
        data = data.explode('payload')
        # Explode entity information
        data = data.join(data['payload'].apply(self.pd.Series))
        data['place_lat'] = data.geometry.apply(lambda x: x['location']['lat'])
        data['place_lon'] = data.geometry.apply(lambda x: x['location']['lng'])
        # Remove irrelevant entities
        data = data.query('business_status == "OPERATIONAL" and permanently_closed != True and user_ratings_total > 0')
        # Remove irrelevant columns and deduplicate entities
        drop_columns = ['ts', 'epicenter_id', 'epicenter_name', 'epicenter_lat', 'epicenter_lon', 'epicenter_radius',
                        'entity_id', 'entity_description', 'entity_type', 'entity_query', 'entity_rankby', 'request',
                        'pages', 'payload', 'geometry', 'icon', 'opening_hours', 'photos', 'plus_code', 'reference',
                        'types', 'business_status', 'permanently_closed']
        data = data.drop(drop_columns, axis=1)
        data = data.drop_duplicates(subset=['place_id']).reset_index(drop=True)
        data.insert(0, 'add_ts', io.now())
        helper.update_pq(data=data, path=self.File.ENTITIES_PROCESSED, dedup=['place_id'])
        print("\nCompleted.")

    def get_unprocessed_data(self):
        print('\nExtract Google Maps API data:')
        helper, io = self.Helper(), self.IO()
        # List of items
        print("\nExtracting...")
        epicenters, entities = io.json_to_df(self.File.EPICENTERS_LIST), io.json_to_df(self.File.ENTITIES_LIST)
        data = self._cartesian_product_basic(entities, epicenters)
        # Check if exists and remove already processed items
        path = self.File.ENTITIES_UNPROCESSED
        dedup_columns = ['entity_id', 'epicenter_id']
        if io.exists(path):
            print('Excluded already extracted data from a new job.')
            data = self._deduplicate_items(data, path, dedup_columns)
        # Check if anything to work on
        if len(data) == 0:
            print('There are no new entities to work on.')
            return
        # Add dummy columns
        data.insert(0, 'ts', None)
        data['request'], data['pages'], data['payload'] = None, None, None
        # Get data from Google
        io.pause(2)
        bar = self.tqdm(total=len(data), bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        io.pause()
        for index in data.index:
            entry = data.loc[index]
            request = self._request_constructor(query=entry.entity_query,
                                                latitude=entry.epicenter_lat,
                                                longitude=entry.epicenter_lon,
                                                rankby=entry.entity_rankby,
                                                radius=entry.epicenter_radius,
                                                entity_type=entry.entity_type)
            payload = self.requests.get(request).json()
            results = payload['results']
            # Check if response is split into several pages and process all of them if that's the case
            pages = 1
            while 'next_page_token' in payload:
                next_page_token = payload['next_page_token']
                request = self._request_constructor(token=next_page_token)
                # "There is a short delay between when a token is issued, and when it will become valid." (c) Google
                io.pause(2)
                payload = self.requests.get(request).json()
                results += payload['results']
                pages += 1
            self.pd.set_option('mode.chained_assignment', None)
            data['ts'][index] = io.now()
            data['payload'][index] = self.json.dumps(results)
            data['pages'][index] = pages
            data['request'][index] = request
            bar.update(1)
        bar.close()
        io.pause()  # Prevents issues with the layout of update messages im terminal
        helper.update_pq(data=data, path=path, dedup=dedup_columns)
        print("\nGoogle Maps API data extraction completed.")

    def _request_constructor(self,
                             query=None,
                             latitude=None,
                             longitude=None,
                             rankby=None,
                             radius=None,
                             entity_type=None,
                             language='en',
                             token=None):
        api = self.API()
        api_key = 'key=' + api.key(service='google', category='maps_key')
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
        io = self.IO()
        old = io.read_pq(path)
        old = old.drop(['ts', 'request', 'pages', 'payload'], axis=1)
        data = self.pd.concat([old, data]) \
            .drop_duplicates(subset=dedup_columns, keep=False) \
            .reset_index(drop=True)
        return data

    def _cartesian_product_basic(self, left, right):
        return left.assign(key=1).merge(right.assign(key=1), on='key').drop('key', 1)


if __name__ == '__main__':
    GoogleMapsEntities().get_processed_data()
