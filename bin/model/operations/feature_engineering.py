class FeatureEngineering(object):

    import re

    from dateutil import relativedelta

    from utils.var import File
    from utils.io import IO

    def building_age(self, data, com=True):
        io = self.IO()
        print('\tBuilding age...', end=' ', flush=True) if com else None
        feature_name = 'building_age'
        feature_metadata = {"name": feature_name, "dtype": "continuous", "used": True}
        year_now = io.now().year
        data = data.apply(lambda x: year_now - int(x.build_year) \
            if x.build_year is not None and 1000 <= int(x.build_year) <= year_now else None, axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def building_new(self, data, threshold=1, com=True):
        io = self.IO()
        print('\tBuilding is new...', end=' ', flush=True) if com else None
        feature_name = 'building_is_new'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        year_now = io.now().year
        data = data.apply(lambda x: 'true' if x.build_year is not None and year_now - int(x.build_year) <= threshold \
                                              and 1000 <= int(x.build_year) <= year_now else 'false', axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def building_fresh(self, data, threshold=5, com=True):
        io = self.IO()
        print('\tBuilding is fresh...', end=' ', flush=True) if com else None
        feature_name = 'building_is_fresh'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        year_now = io.now().year
        data = data.apply(lambda x: 'true' if x.build_year is not None and year_now - int(x.build_year) <= threshold \
                                              and 1000 <= int(x.build_year) <= year_now else 'false', axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def building_historic(self, data, threshold=80, com=True):
        io = self.IO()
        print('\tBuilding is historic...', end=' ', flush=True) if com else None
        feature_name = 'building_is_historic'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        year_now = io.now().year
        data = data.apply(lambda x: 'true' if x.build_year is not None and year_now - int(x.build_year) >= threshold \
                                              and 1000 <= int(x.build_year) <= year_now else 'false', axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def building_century(self, data, com=True):
        io = self.IO()
        print('\tBuilding century...', end=' ', flush=True) if com else None
        feature_name = 'building_century'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        year_now = io.now().year
        data = data.apply(lambda x: int(x.build_year) // 100 + 1 \
            if x.build_year is not None and 1000 <= int(x.build_year) <= year_now else None, axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def threshold(self, data, threshold_number=None, threshold_quantile=0.05):
        io = self.IO()
        BOOLEAN_THRESHOLDS = self.File.BOOLEAN_THRESHOLDS
        column_name = data.columns[0]
        feature_name = column_name + '_boolean'
        if io.exists(BOOLEAN_THRESHOLDS):
            threshold_library = io.read_csv(path=BOOLEAN_THRESHOLDS)
            if column_name in list(threshold_library.name):
                threshold = threshold_library.query("name == @column_name")['value'].iloc[0]
                expected = threshold if threshold is not None else data.quantile(threshold_quantile)[0]
                if threshold != expected:
                    threshold = expected
                    threshold_library = threshold_library[threshold_library.name != column_name]
                    threshold_library.to_csv(path_or_buf=BOOLEAN_THRESHOLDS, sep=';', index=False, header=True)
                    io.append_csv(path=BOOLEAN_THRESHOLDS, fields=[column_name, expected])
            else:
                threshold = threshold_number if threshold_number is not None else data.quantile(threshold_quantile)[0]
                io.append_csv(BOOLEAN_THRESHOLDS, [column_name, threshold])
        else:
            threshold = threshold_number if threshold_number is not None else data.quantile(threshold_quantile)[0]
            io.create_csv(path=BOOLEAN_THRESHOLDS, header=['name', 'value'])
            io.append_csv(path=BOOLEAN_THRESHOLDS, fields=[column_name, threshold])
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: 'true' if x[column_name] > threshold else 'false', axis=1)
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def year(self, data, name, com=True):
        print(f'\t{name.capitalize()} year...', end=' ', flush=True) if com else None
        column_name = data.columns[0]
        feature_name = name + '_year'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: x[column_name].year if x[column_name] is not None else None, axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def month(self, data, name, com=True):
        print(f'\t{name.capitalize()} month...', end=' ', flush=True) if com else None
        column_name = data.columns[0]
        feature_name = name + '_month'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: x[column_name].month if x[column_name] is not None else None, axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def day(self, data, name, com=True):
        print(f'\t{name.capitalize()} day...', end=' ', flush=True) if com else None
        column_name = data.columns[0]
        feature_name = name + '_day'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: x[column_name].day if x[column_name] is not None else None, axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def dayofweek(self, data, name, com=True):
        print(f'\t{name.capitalize()} day of the week...', end=' ', flush=True) if com else None
        column_name = data.columns[0]
        feature_name = name + '_dayofweek'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: x[column_name].dayofweek if x[column_name] is not None else None, axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def weekend(self, data, name, com=True):
        print(f'\t{name.capitalize()} weekend...', end=' ', flush=True) if com else None
        column_name = data.columns[0]
        feature_name = name + '_weekend'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: 'true' if x[column_name] is not None \
                                              and x[column_name].dayofweek in [6, 7] else 'false', axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def floor_number(self, data, threshold_floor=30, com=True):
        print('\tFloor number...', end=' ', flush=True) if com else None
        feature_name = 'floor_number'
        feature_metadata = {"name": feature_name, "dtype": "continuous", "used": True}
        data = data.apply(lambda x: self._parse_floor(str(x.address), threshold_floor) \
            if x.address is not None else None, axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def ground_floor(self, data, threshold_floor=30, ground_floor=None, com=True):
        print('\tGround floor...', end=' ', flush=True) if com else None
        feature_name = 'ground_floor'
        ground_floor = [0, 1] if ground_floor is None else ground_floor
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        if 'floor_number' in data.columns:
            data = 'true' if data.floor_number.item() in ground_floor else 'false'
        else:
            data = data.apply(lambda x: self._is_ground_floor(str(x.address), threshold_floor, ground_floor) \
                if x.address is not None else None, axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

    def address_street_building(self, data, com=True):
        print('\tStreet and building number...', end=' ', flush=True) if com else None
        feature_name = 'address_street_building'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        output = data.apply(lambda x: str(x.gmaps_route) + ' ' + str(x.gmaps_street_number) \
            if x.gmaps_route is not None and x.gmaps_street_number is not None else None, axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": output}

    def postal_code_area(self, data, com=True):
        print('\tPostal code area...', end=' ', flush=True) if com else None
        feature_name = 'postal_code_area'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        output = data.apply(lambda x: str(x.gmaps_postal_code)[:3] \
            if x.gmaps_postal_code is not None and len(x.gmaps_postal_code) == 6 else None, axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": output}

    def district_clean(self, data, com=True):
        print('\tDistrict cleaned...', end=' ', flush=True) if com else None
        feature_name = 'district_clean'
        feature_metadata = {"name": feature_name, "dtype": "categorical", "used": True}
        data = data.apply(lambda x: self._district_parser(str(x.district)) \
            if x.district is not None else None, axis=1)
        print('Done.') if com else None
        return {"name": feature_name, "metadata": feature_metadata, "content": data}

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
