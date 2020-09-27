class PayloadProcessing(object):

    import pandas as pd

    from bin.queries.layers.destinations import Destinations
    from bin.queries.layers.entities import Entities
    from bin.queries.layers.transport import Transport
    from bin.queries.layers.address import Address
    from bin.queries.layers.clustering import Clustering

    from bin.model.preprocessing import Preprocessing
    from bin.model.operations.feature_engineering import FeatureEngineering

    from utils.var import File
    from utils.data import Data
    from utils.io import IO

    def enrichment(self, data):
        io, fe = self.IO(), self.FeatureEngineering()
        # Consume  request
        data['country'], data['sold_at_date'] = 'Sweden', io.now()  # Default predefined values
        data = self._enrich_data(data)
        # Get predefined metadata
        metadata = self.pd.read_csv(self.File.FEATURES_METADATA, delimiter=';')
        # Get engineered metadata
        continuous_count = ['entities_', 'lines_', 'stops_', 'points_']
        continuous_distance = ['distance_to_', 'nearest_', ]
        continuous_scores = ['scores_mean_', 'scores_median_']
        continuous_reviews = ['reviews_mean_', 'reviews_median_']
        continuous = continuous_count + continuous_distance + continuous_scores + continuous_reviews
        engineered_columns = self.Preprocessing().columns_matcher(data=data, pattern=continuous)
        for column in engineered_columns:
            metadata = metadata.append(self.Preprocessing().map_dtype(name=str(column), continuous=continuous,
                                                                      exceptions=metadata['name']), ignore_index=True)
        # Feature engineering
        self.pd.set_option('mode.chained_assignment', None)
        features = self._calculate_features(data)
        for column in self.Preprocessing().columns_matcher(data=data, pattern=continuous_count):
            features += [fe.threshold(data[[column]])]  # Binary features
        # Extend metadata
        for feature in features:
            data[feature['name']] = feature['content']
            metadata = metadata.append(feature['metadata'], ignore_index=True)
        # Filter values and groom columns
        data = self.Preprocessing().groom_dtype(data=data,
                                                dtypes=dict(zip(metadata.name, metadata.dtype)),
                                                exceptions=['sold_at_date'],
                                                com=False)
        # Return enriched request
        selected_features = self.Data().features()
        return data[selected_features]

    def _enrich_data(self, data):
        # Form a request to Google Maps API
        data['gmaps_address_request'] = self._query_constructor(data)
        # Enrich: Address
        address = self.Address().request_address(data[['gmaps_address_request']])
        data[['latitude', 'longitude']] = address[['gmaps_latitude', 'gmaps_longitude']]
        # Enrichment: Clustering
        clusters = self.Clustering().request_clustering(data[['latitude', 'longitude']])
        # Enrichment: Destinations
        destinations = self.Destinations().request_destinations(data[['latitude', 'longitude']])
        # Enrich: Entities (Google Maps)
        entities = self.Entities().request_entities(data[['latitude', 'longitude']])
        # Enrich: Transport
        transport = self.Transport().request_transport(data[['latitude', 'longitude']])
        # Merge enrichment layers
        data = self.pd.concat([data, address, clusters, destinations, entities, transport], axis=1)
        return data

    def _calculate_features(self, data):
        fe = self.FeatureEngineering()
        return [
            fe.building_age(data=data[['build_year']], com=False),
            fe.building_century(data=data[['build_year']], com=False),
            fe.building_new(data=data[['build_year']], com=False),
            fe.building_fresh(data=data[['build_year']], com=False),
            fe.building_historic(data=data[['build_year']], com=False),
            fe.year(data=data[['sold_at_date']], name='sold', com=False),
            fe.month(data=data[['sold_at_date']], name='sold', com=False),
            fe.day(data=data[['sold_at_date']], name='sold', com=False),
            fe.dayofweek(data=data[['sold_at_date']], name='sold', com=False),
            fe.weekend(data=data[['sold_at_date']], name='sold', com=False),
            fe.ground_floor(data=data[['floor_number']], com=False),
            fe.address_street_building(data=data[['gmaps_route', 'gmaps_street_number']], com=False),
            fe.postal_code_area(data=data[['gmaps_postal_code']], com=False)
        ]

    def _query_constructor(self, data):
        return f'{data.street.item()} {data.building.item()}, {data.zipcode.item()} {data.city.item()}, ' \
               f'{data.country.item()}'
