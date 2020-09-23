class FeatureEngineering(object):

    import gender_guesser.detector as gender
    import pandas as pd
    import matplotlib.pyplot as plt
    import re
    import seaborn as sns

    from dateutil import relativedelta
    from sklearn.cluster import KMeans

    from utils.var import File
    from utils.io import IO

    # TODO: Add BRF via geopy library and address

    def broker_gender(self, data):
        print('\tBroker gender...', end=' ', flush=True)
        feature_name = 'broker_gender'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: self._identify_gender(x.broker_full_name[0]), axis=1)
        data = data.replace({'andy': None, 'mostly_male': None, 'mostly_female': None})
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def building_age(self, data):
        io = self.IO()
        print('\tBuilding age...', end=' ', flush=True)
        feature_name = 'building_age'
        feature_metadata = {"name": feature_name, "dtype": "continuous", "used": True}
        year_now = io.now().year
        data = data.apply(lambda x: year_now - int(x.build_year) \
            if x.build_year is not None and 1000 <= int(x.build_year) <= year_now else None, axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def building_new(self, data, threshold=1):
        io = self.IO()
        print('\tBuilding is new...', end=' ', flush=True)
        feature_name = 'building_is_new'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        year_now = io.now().year
        data = data.apply(lambda x: 'true' if x.build_year is not None and year_now - int(x.build_year) <= threshold \
                                              and 1000 <= int(x.build_year) <= year_now else 'false', axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def building_fresh(self, data, threshold=5):
        io = self.IO()
        print('\tBuilding is fresh...', end=' ', flush=True)
        feature_name = 'building_is_fresh'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        year_now = io.now().year
        data = data.apply(lambda x: 'true' if x.build_year is not None and year_now - int(x.build_year) <= threshold \
                                              and 1000 <= int(x.build_year) <= year_now else 'false', axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def building_historic(self, data, threshold=80):
        io = self.IO()
        print('\tBuilding is historic...', end=' ', flush=True)
        feature_name = 'building_is_historic'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        year_now = io.now().year
        data = data.apply(lambda x: 'true' if x.build_year is not None and year_now - int(x.build_year) >= threshold \
                                              and 1000 <= int(x.build_year) <= year_now else 'false', axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def building_century(self, data):
        io = self.IO()
        print('\tBuilding century...', end=' ', flush=True)
        feature_name = 'building_century'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        year_now = io.now().year
        data = data.apply(lambda x: int(x.build_year) // 100 + 1 \
            if x.build_year is not None and 1000 <= int(x.build_year) <= year_now else None, axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def threshold(self, data, threshold_number=None, threshold_quantile=0.05):
        column_name = data.columns[0]
        threshold = threshold_number if threshold_number is not None else data.quantile(threshold_quantile)[0]
        # print(f'\tBoolean for {column_name} (threshold = {round(threshold,1)})...', end=' ', flush=True)
        feature_name = column_name + '_boolean'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: 'true' if x[column_name] > threshold else 'false', axis=1)
        # print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def year(self, data, name):
        print(f'\t{name.capitalize()} year...', end=' ', flush=True)
        column_name = data.columns[0]
        feature_name = name + '_year'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: x[column_name].year if x[column_name] is not None else None, axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def month(self, data, name):
        print(f'\t{name.capitalize()} month...', end=' ', flush=True)
        column_name = data.columns[0]
        feature_name = name + '_month'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: x[column_name].month if x[column_name] is not None else None, axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def day(self, data, name):
        print(f'\t{name.capitalize()} day...', end=' ', flush=True)
        column_name = data.columns[0]
        feature_name = name + '_day'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: x[column_name].day if x[column_name] is not None else None, axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def dayofweek(self, data, name):
        print(f'\t{name.capitalize()} day of the week...', end=' ', flush=True)
        column_name = data.columns[0]
        feature_name = name + '_dayofweek'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: x[column_name].dayofweek if x[column_name] is not None else None, axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def weekend(self, data, name):
        print(f'\t{name.capitalize()} weekend...', end=' ', flush=True)
        column_name = data.columns[0]
        feature_name = name + '_weekend'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: 'true' if x[column_name] is not None \
                                              and x[column_name].dayofweek in [6, 7] else 'false', axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def floor_number(self, data, threshold_floor=30):
        print('\tFloor number...', end=' ', flush=True)
        feature_name = 'floor_number'
        feature_metadata = {"name": feature_name, "dtype": "continuous", "used": True}
        data = data.apply(lambda x: self._parse_floor(str(x.address), threshold_floor) \
            if x.address is not None else None, axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def ground_floor(self, data, threshold_floor=30, ground_floor=None):
        print('\tGround floor...', end=' ', flush=True)
        feature_name = 'ground_floor'
        ground_floor = [0, 1] if ground_floor is None else ground_floor
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: self._is_ground_floor(str(x.address), threshold_floor, ground_floor) \
            if x.address is not None else None, axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def address_street_building(self, data):
        print('\tStreet and building number...', end=' ', flush=True)
        feature_name = 'address_street_building'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        output = data.apply(lambda x: str(x.gmaps_route) + ' ' + str(x.gmaps_street_number) \
            if x.gmaps_route is not None and x.gmaps_street_number is not None else None, axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": output}

    def postal_code_area(self, data):
        print('\tPostal code area...', end=' ', flush=True)
        feature_name = 'postal_code_area'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        output = data.apply(lambda x: str(x.gmaps_postal_code)[:3] \
            if x.gmaps_postal_code is not None and len(x.gmaps_postal_code) == 6 else None, axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": output}

    def district_clean(self, data):
        print('\tDistrict cleaned...', end=' ', flush=True)
        feature_name = 'district_clean'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: self._district_parser(str(x.district)) \
            if x.district is not None else None, axis=1)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def clustering(self, data, n_clusters=50):
        n_clusters = len(data) if n_clusters > len(data) else n_clusters  # n_clusers must be >= n_samples
        print('\tClustering by geodata...', end=' ', flush=True)
        feature_name = 'cluster'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data[['lat', 'lng']] = self.pd.DataFrame(data.coordinates.tolist(), index=data.index)
        kmeans = self.KMeans(n_clusters=n_clusters, init='k-means++')
        kmeans.fit(data[['lat', 'lng']])  # Compute k-means clustering
        data[['cluster']] = kmeans.fit_predict(data[['lat', 'lng']])
        self.plot_clusters(data[['lat', 'lng', 'cluster']], path=self.File.PROPERTY_CLUSTERING_REPORT)
        print('Done.')
        return {"name": feature_name, "metadata": feature_metadata, "content": data['cluster']}

    # Helpers

    def _identify_gender(self, name):
        return self.gender.Detector().get_gender(f"{name}", u'sweden')

    def _time_diff(self, start, end):
        return self.relativedelta.relativedelta(start, end)

    def _parse_floor(self, address, threshold_floor):
        list_address = address.split(',')
        if len(list_address) > 1:
            clean = self.re.sub('\([^)]*\)', '', list_address[1])
            floor = self.re.sub('[^0-9]', '', clean)
            return floor if floor and int(floor) <= threshold_floor else None

    def _is_ground_floor(self, address, threshold_floor, ground_floor):
        floor = self._parse_floor(address, threshold_floor)
        if floor:
            return 'true' if floor in ground_floor else 'false'
        else:
            return None

    def _building_number(self, address):
        list_address = address.split(',')
        return self.re.sub("[^0-9]", "", list_address[-1]) if len(list_address) > 0 else ''

    def _district_parser(self, district):
        separators = [',', '/', '-']
        district = district.replace('Stockholm - inom tullarna,', '').replace('Stockholm,', '').replace('Stockholm', '')
        for i in separators:
            district_list = district.split(i)
            if len(district_list) == 0:
                district = district_list[0].strip()
                return district if len(district) > 0 else None
            else:
                district = district_list[0].strip()
                return district if len(district) > 0 else None

    def plot_clusters(self, data, path):
        io = self.IO()
        directory, name = io.dir_and_base(path)
        self.sns.set()
        plot = self.sns.scatterplot(x='lat', y='lng', data=data,
                                    hue=data.cluster.tolist(), legend=False, palette='muted')
        self.plt.title('Property Clustering')
        self.plt.xlabel('Latitude')
        self.plt.ylabel('Longitude')
        io.make_dir(directory)
        plot.figure.savefig(path, dpi=900)
        self.plt.close()
