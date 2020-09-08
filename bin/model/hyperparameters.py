class Hyperparameters(object):

    import json
    import numpy as np
    import pandas as pd
    import warnings

    from bayes_opt import BayesianOptimization
    from datetime import datetime
    from lightgbm.sklearn import LGBMClassifier
    from os import path
    from pathlib import Path

    from bin.helpers.helper import Helper
    from utils.files import Utils

    warnings.simplefilter(action='ignore', category=UserWarning)

    def hyperparameters_tuning(self):
        print('\nRun hyperparameters optimisation:')
        start = self.datetime.now().replace(microsecond=0)

        # Get data and features
        print('\n\tGet data and a list of features...', end=' ', flush=True)
        x_train, y_train = self._get_processed_data()
        features = self._get_features()
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
        self._save_results(optimizer, start)
        print("\nHyperparameters optimisation was successfully completed.")

    def _get_processed_data(self):
        helper, utils = self.Helper(), self.Utils()
        # Get path
        x_path = utils.get_full_path('data/dataset/processed/x_test')
        y_path = utils.get_full_path('data/dataset/processed/y_test')
        # Check if exists
        exists = all([helper.exists(x_path), helper.exists(y_path)])
        if not exists:
            print('Sorry, datasets does not exist. Please, run preprocessing first.')
            return
        # Convert to DataFrame
        x, y = self.pd.read_parquet(x_path, engine="fastparquet"), self.pd.read_parquet(y_path, engine="fastparquet")
        return x, y.values.ravel()

    def _get_features(self):
        helper, utils = self.Helper(), self.Utils()
        # Get path
        path = utils.get_full_path('data/dataset/processed/operations/features.txt')
        # Check if exists
        exists = helper.exists(path)
        if not exists:
            print('Sorry, feature list does not exist. Please, run feature selection first.')
            return
        with open(path, 'r') as f:
            features = f.read().split('\n')
        return features

    def _cv_score(self, x_train, y_train, learning_rate=0.1, num_leaves=200, max_depth=3, n_estimators=500,
                  min_child_samples=100, colsample_bytree=1, scale_pos_weight=1):
        from sklearn.model_selection import cross_val_score
        # Ensure parameters are provided in valid format
        num_leaves = int(num_leaves)
        max_depth = int(max_depth)
        n_estimators = int(n_estimators)
        min_child_samples = int(min_child_samples)
        # Train model
        classifier = self.LGBMClassifier(num_leaves=num_leaves, max_depth=max_depth, learning_rate=learning_rate,
                                         n_estimators=n_estimators, min_child_samples=min_child_samples,
                                         colsample_bytree=colsample_bytree, silent=True, objective="regression",
                                         boosting_type='gbdt', scale_pos_weight=scale_pos_weight, n_jobs=-1,
                                         nthread=None, random_state=28, seed=None, natural_bad_rate=None)
        # Calculate scores
        scores = cross_val_score(classifier, x_train, y_train, cv=5, scoring='neg_root_mean_squared_error')
        return self.np.mean(scores)

    def _save_results(self, optimizer, start):
        helper, utils = self.Helper(), self.Utils()
        directory = utils.get_full_path('data/dataset/processed/operations')
        path = self.path.join(directory, 'bayesian_hyperparameters.json')
        self.Path(directory).mkdir(parents=True, exist_ok=True)
        duration = self.datetime.now().replace(microsecond=0) - start
        data = {'ts': str(start), 'duration': str(duration), 'hyperparameters': optimizer.max}
        if helper.exists(path):
            with open(path, 'a') as f:
                f.write(self.json.dumps(data) + "\n")
        else:
            with open(path, 'w+') as f:
                f.write(self.json.dumps(data) + "\n")


if __name__ == '__main__':
    Hyperparameters().hyperparameters_tuning()
