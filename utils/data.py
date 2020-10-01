class Data(object):

    import json

    from utils.var import File
    from utils.io import IO

    def processed(self, parts=None, extra=True):
        io = self.IO()
        if parts is None:
            parts = ['train', 'val', 'test']

        x_train = io.read_pq(self.File.X_TRAIN)
        y_train = io.read_pq(self.File.Y_TRAIN)
        e_train = io.read_pq(self.File.E_TRAIN)

        x_val = io.read_pq(self.File.X_VAL)
        y_val = io.read_pq(self.File.Y_VAL)
        e_val = io.read_pq(self.File.E_VAL)

        x_test = io.read_pq(self.File.X_TEST)
        y_test = io.read_pq(self.File.Y_TEST)
        e_test = io.read_pq(self.File.E_TEST)

        data = {}

        if 'train' in parts:
            data['x_train'] = x_train
            data['y_train'] = y_train
            if extra:
                data['e_train'] = e_train
        if 'val' in parts:
            data['x_val'] = x_val
            data['y_val'] = y_val
            if extra:
                data['e_val'] = e_val
        if 'test' in parts:
            data['x_test'] = x_test
            data['y_test'] = y_test
            if extra:
                data['e_test'] = e_test
        return data

    def hyperparameters(self):
        io = self.IO()
        if not io.exists(self.File.HYPERPARAMETERS):
            print('\nSorry, hyperparameters file does not exist. Please, run hyperparameters tuning first.')
            return
        data = io.read_csv(self.File.HYPERPARAMETERS)
        hypers = self.json.loads(data.iloc[-1]['hyperparameters'].replace("\'", "\""))['params']
        for param in ['num_leaves', 'max_depth', 'n_estimators', 'min_child_samples']:
            if param in hypers:
                hypers[param] = int(hypers[param])
        return hypers

    def features(self):
        io = self.IO()
        if not io.exists(self.File.SELECTED_FEATURES):
            print('\nSorry, feature list does not exist. Please, run feature selection first.')
            return
        with open(self.File.SELECTED_FEATURES, 'r') as f:
            features = f.read().split('\n')
        return features
