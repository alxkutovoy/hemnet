class Train(object):

    import warnings

    from lightgbm.sklearn import LGBMRegressor

    from utils.data import Data
    from utils.var import File
    from utils.io import IO

    warnings.simplefilter(action='ignore', category=UserWarning)

    def train(self):
        print('\nTrain the model:')
        io = self.IO()
        # Get data
        print('\tGet data...', end=' ', flush=True)
        data = self.Data().processed()
        x_train, x_val, y_train, y_val = data['x_train'], data['x_val'], data['y_train'], data['y_val']
        print('Done.')
        # Select relevant features
        print('\tSelect features...', end=' ', flush=True)
        features = self.Data().features()
        x_train, x_val = x_train[features], x_val[features]
        print('Done.')
        # Get hyperparameters
        print('\tGet hyperparameters...', end=' ', flush=True)
        hyperparameters = self.Data().hyperparameters()
        print('Done.')
        # Fit the model
        print('\tFit model...', end=' ', flush=True)
        model = self.LGBMRegressor(**hyperparameters)
        model.fit(x_train, y_train, eval_set=[(x_val, y_val)], verbose=False)
        print('Done.')
        # Save results
        print('\tSave model...', end=' ', flush=True)
        io.save_pkl(model=model, path=self.File.MODEL)
        print('Done.')
        # End
        print('\nModel training was successfully completed.')


if __name__ == '__main__':
    Train().train()
