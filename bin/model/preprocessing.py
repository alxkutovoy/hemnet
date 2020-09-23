class Preprocessing(object):

    import numpy as np
    import pandas as pd

    from bin.model.eda import EDA
    from bin.model.operations.feature_engineering import FeatureEngineering
    from utils.var import File, Dir
    from utils.io import IO

    def preprocessing(self):
        print('\nPreprocess dataset:')
        io, fe, eda = self.IO(), self.FeatureEngineering(), self.EDA()
        # Get initial data
        data = io.read_pq(self.File.ENRICHED_SUBSET)
        # Special columns
        ts_column, target_column, index_column = 'sold_at_date', 'end_price', 'sold_property_id'
        # Get predefined metadata
        metadata = self.pd.read_csv(self.File.FEATURES_METADATA, delimiter=';')
        # Get engineered metadata
        continuous_count = ['entities_', 'lines_', 'stops_', 'points_']
        continuous_distance = ['distance_to_', 'nearest_', ]
        continuous_scores = ['scores_mean_', 'scores_median_']
        continuous_reviews = ['reviews_mean_', 'reviews_median_']
        continuous_other = ['gmaps_lattitude', 'gmaps_longitude']
        continuous = continuous_count + continuous_distance + continuous_scores + continuous_reviews + continuous_other
        engineered_columns = self.columns_matcher(data=data, pattern=continuous)
        for column in engineered_columns:
            metadata = metadata.append(self.map_dtype(name=str(column), continuous=continuous,
                                                      exceptions=metadata['name']), ignore_index=True)
        # Feature engineering
        print('\nFeature engineering:')
        self.pd.set_option('mode.chained_assignment', None)
        # Distinct features
        features = [
            fe.broker_gender(data=data[['broker_full_name']]),
            fe.building_age(data=data[['build_year']]),
            fe.building_century(data=data[['build_year']]),
            fe.building_new(data=data[['build_year']]),
            fe.building_fresh(data=data[['build_year']]),
            fe.building_historic(data=data[['build_year']]),
            fe.year(data=data[[ts_column]], name='sold'),
            fe.month(data=data[[ts_column]], name='sold'),
            fe.day(data=data[[ts_column]], name='sold'),
            fe.dayofweek(data=data[[ts_column]], name='sold'),
            fe.weekend(data=data[[ts_column]], name='sold'),
            fe.floor_number(data=data[['address']]),
            fe.ground_floor(data=data[['address']]),
            fe.address_street_building(data=data[['gmaps_route', 'gmaps_street_number']]),
            fe.postal_code_area(data=data[['gmaps_postal_code']]),
            fe.district_clean(data=data[['district']]),
            fe.clustering(data=data[['coordinates']])
        ]
        # Series of features
        continuous_count_columns = self.columns_matcher(data=data, pattern=continuous_count)
        print(f'\tBooleans for {len(continuous_count_columns)} columns...', end=' ', flush=True)
        for column in self.columns_matcher(data=data, pattern=continuous_count):
            features += [fe.threshold(data[[column]])]
        print('Done.')
        # Extend metadata
        for feature in features:
            data[feature['name']] = feature['content']
            metadata = metadata.append(feature['metadata'], ignore_index=True)
        # Data processing
        print('\nData preprocessing:')
        # Filter values and groom columns
        data = self.groom_dtype(data=data,
                                dtypes=dict(zip(metadata.name, metadata.dtype)),
                                exceptions=[ts_column],
                                target_column=target_column)
        # Split into train, validation and test sets
        datasets = self._create_partitions(data=data, ts_column=ts_column)
        train_data, validation_data, test_data = [datasets[x] for x in ['train', 'test', 'oot']]
        # Split into features and lables
        print('\tSplit into features and lables...', end=' ', flush=True)
        x_train, y_train, extras_train = self._feature_label(data=train_data, meta=metadata, target=target_column)
        x_val, y_val, extras_val = self._feature_label(data=validation_data, meta=metadata, target=target_column)
        x_test, y_test, extras_test = self._feature_label(data=test_data, meta=metadata, target=target_column)
        print('Done.')
        # Save data
        print('\tSave data...', end=' ', flush=True)
        directory = self.Dir.PREPROCESSED
        self._save_dataset(directory=directory, name='train', x=x_train, y=y_train, e=extras_train)
        self._save_dataset(directory=directory, name='val', x=x_val, y=y_val, e=extras_val)
        self._save_dataset(directory=directory, name='test', x=x_test, y=y_test, e=extras_test)
        print('Done.')
        # Create report for x_train
        eda.report(data=x_train, name='x_train', comments=True, sample=1000)
        print('\nPreprocessing was successfully completed.')

    # Helpers

    def _create_partitions(self, data, ts_column, partitions=None):
        print('\tSplit into train, validation and test...', end=' ', flush=True)
        from sklearn.model_selection import train_test_split
        datasets = {}
        if not partitions:
            partitions = self._partitions(data[ts_column])
        for part in partitions:
            specification = partitions[part]
            start, end = specification['window']
            data_part = data[(start <= data[ts_column]) & (data[ts_column] < end)]
            test_size = specification['test_size']
            if test_size < 1:
                train_data, test_data = train_test_split(data_part, test_size=test_size, random_state=28)
                datasets['train'] = train_data
                datasets['test'] = test_data
            else:
                datasets[part] = data_part
        print('Done.')
        return datasets

    def groom_dtype(self, data, dtypes, missing_category=False, exceptions=None, target_column=None):
        print('\tFilter values and groom columns...', end=' ', flush=True)
        replace_values = [None, '']
        for feature, value in dtypes.items():
            if exceptions is not None and feature in exceptions:
                continue
            # Convert lists into strings
            if feature in data.columns and type(data[feature].iloc[0]) == list:
                data[feature] = data[feature].str.join(' ')
            # Normalise target variable
            if feature == target_column:
                from sklearn.preprocessing import FunctionTransformer
                transformer = FunctionTransformer(self.np.log1p, validate=True)
                data[target_column] = transformer.transform(data[[target_column]])
            if feature in data.columns:
                if value == 'continuous':
                    data[feature] = self.pd.to_numeric(data[feature], errors='coerce') \
                        .replace(replace_values, self.np.nan).astype('float64')
                elif value == 'categorical':
                    if type(data[feature].iloc[0]) in [int, float, self.np.float64]:
                        data[feature] = data[feature].apply(str).replace('.0', '')
                    fill_value = 'Missing' if missing_category else self.np.nan
                    data[feature] = data[feature].replace(replace_values, fill_value).astype('category')
                else:
                    fill_value = 'Missing' if missing_category else self.np.nan
                    data[feature] = data[feature].replace(replace_values, fill_value).astype('category')
        print('Done.')
        return data

    def _feature_label(self, data, meta, target):
        data = data.reset_index(drop=True)
        modeling_features = meta.query('used == True').name.tolist()
        non_modeling_features = list(set(data.columns) - set(modeling_features) - {target})
        # Ensure columns exists in dataset
        modeling_features = list(set(modeling_features).intersection(list(data.columns)))
        non_modeling_features = list(set(non_modeling_features).intersection(list(data.columns)))
        # Split into features, labels and extras
        x = data.loc[:, modeling_features]
        y = data.loc[:, target].rename('target')
        e = data.loc[:, non_modeling_features]
        return [x, y, e]

    def _partitions(self, data, train_size=0.8, test_size=0.3):
        split = data.quantile([0, train_size, 1])
        train_0, oot_0, oot_1 = str(split[0].date()), str(split[0.8].date()), str(split[1].date())
        return {
            'train_test':
                {'window': [train_0, oot_0],
                 'test_size': test_size},
            'oot':
                {'window': [oot_0, oot_1],
                 'test_size': 1}
        }

    def _save_dataset(self, directory, name, x, y, e):
        io = self.IO()
        io.make_dir(directory)
        x_name, y_name, e_name = 'x_' + name, 'y_' + name, 'e_' + name
        io.save_pq(data=x, path=io.path_join(directory, x_name))
        io.save_pq(data=self.pd.DataFrame(y), path=io.path_join(directory, y_name))
        io.save_pq(data=e, path=io.path_join(directory, e_name))

    def columns_matcher(self, data, pattern):
        return [i for i in list(data.columns) if any(i for j in pattern if str(j) in i)]

    def map_dtype(self, name, continuous=None, categorical=None, exceptions=None):
        if exceptions is not None and exceptions.str.contains(name).any():
            return
        if self._contains(name, continuous):
            dtype = 'continuous'
            used = True
        elif self._contains(name, categorical):
            dtype = 'categorical'
            used = True
        else:
            dtype = 'categorical'
            used = False
        return {"name": name, "dtype": dtype, "used": used}

    def _contains(self, name, patterns):
        return any(pattern in name for pattern in patterns)


if __name__ == '__main__':
    Preprocessing().preprocessing()
