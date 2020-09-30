class Hyperparameters(object):

    import numpy as np
    import warnings

    from bayes_opt import BayesianOptimization
    from lightgbm.sklearn import LGBMRegressor

    from utils.data import Data
    from utils.var import File
    from utils.io import IO

    warnings.simplefilter(action='ignore', category=UserWarning)

    def hyperparameters_tuning(self):
        io = self.IO()
        print('\nRun hyperparameters optimisation:')
        start = io.now()
        # Get data and features
        print('\n\tGet data and a list of features...', end=' ', flush=True)
        data = self.Data().processed()
        x_train, y_train = data['x_train'], data['y_train'].values.ravel()
        features = self.Data().features()
        x_train = x_train[features]
        print('Done.')
        # Define boundaries
        print('\tDefine boundaries and a function...', end=' ', flush=True)
        pbounds = {'learning_rate': (0.01, 0.2),
                   'num_leaves': (10, 500),
                   'max_depth': (2, 7),
                   'n_estimators': (300, 600),
                   'min_child_samples': (100, 1000),
                   'colsample_bytree': (0.5, 1.0)
                   }
        def f(**kwargs):
            return self._cv_score(x_train, y_train, **kwargs)
        print('Done.')
        # Run optimiser
        print('\n\tRun BayesianOptimization...\n')
        optimizer = self.BayesianOptimization(f=f, pbounds=pbounds, random_state=28)
        optimizer.maximize(init_points=5, n_iter=100)
        # Save result
        self._save_results(optimizer=optimizer, start=start, entries=len(x_train))
        print("\nHyperparameters optimisation was successfully completed.")

    def _cv_score(self, x_train, y_train, learning_rate=0.1, num_leaves=200, max_depth=3, n_estimators=500,
                  min_child_samples=100, colsample_bytree=1, scale_pos_weight=1):
        from sklearn.model_selection import cross_val_score
        # Ensure parameters are provided in valid format
        num_leaves = int(num_leaves)
        max_depth = int(max_depth)
        n_estimators = int(n_estimators)
        min_child_samples = int(min_child_samples)
        # Train model
        estimator = self.LGBMRegressor(num_leaves=num_leaves, max_depth=max_depth, learning_rate=learning_rate,
                                       n_estimators=n_estimators, min_child_samples=min_child_samples,
                                       colsample_bytree=colsample_bytree, silent=True, objective="regression",
                                       boosting_type='gbdt', scale_pos_weight=scale_pos_weight, n_jobs=-1,
                                       nthread=None, random_state=28, seed=None, natural_bad_rate=None)
        # Calculate scores
        scores = cross_val_score(estimator, x_train, y_train, cv=5, scoring='neg_root_mean_squared_error')
        return self.np.mean(scores)

    def _save_results(self, optimizer, start, entries):
        io = self.IO()
        path = self.File.HYPERPARAMETERS
        io.make_dir(io.dir(path))
        data = [str(start), str(io.now() - start), entries, optimizer.max['target'], optimizer.max]
        if io.exists(path):
            io.append_csv(path=path, fields=data)
        else:
            header = ['ts', 'duration', 'entries', 'target', 'hyperparameters']
            io.create_csv(path=path, header=header)
            io.append_csv(path=path, fields=data)


if __name__ == '__main__':
    Hyperparameters().hyperparameters_tuning()
