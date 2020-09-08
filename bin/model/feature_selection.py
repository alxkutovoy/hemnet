class FeatureSelection(object):

    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    import seaborn as sns

    from lightgbm.sklearn import LGBMClassifier
    from os import path
    from pathlib import Path

    from bin.helpers.helper import Helper
    from utils.files import Utils

    def feature_selection(self, threshold=0.9, criterion='neg_root_mean_squared_error'):
        from sklearn.inspection import permutation_importance
        print('\nSelect features:')
        # Get data
        x_train, x_val, y_train, y_val = self._get_processed_data()
        # Define classifier and fit data
        print('\n\tDefine classifier and fit data...', end=' ', flush=True)
        pipe = self.LGBMClassifier()
        pipe.fit(x_train, y_train.values.ravel())
        print('Done.')
        # Run permutations
        print('\tRun permutations...', end=' ', flush=True)
        results = permutation_importance(estimator=pipe, X=x_val, y=y_val,
                                         scoring=criterion, n_jobs=-1, random_state=28, n_repeats=10)
        print('Done.')
        # Process results
        features = self._evaluate(results=results, threshold=threshold, columns=x_train.columns)
        # Plot results
        self._illustrator(features)
        # Select features
        self._select_features(features)
        print('\nFeature selection was successfully completed.')

    def _get_processed_data(self, test_size=0.2, random_state=28):
        from sklearn.model_selection import train_test_split
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
        # Split
        x_train, x_val, y_train, y_val = train_test_split(x, y, test_size=test_size, random_state=random_state)
        return x_train, x_val, y_train, y_val

    def _evaluate(self, results, threshold, columns):
        print('\tProcess results...', end=' ', flush=True)
        output = self.pd.DataFrame({'feature': columns, 'importance': results.importances_mean}) \
            .sort_values('importance', ascending=False, ignore_index=True)
        output['cumulative'] = self.np.cumsum(output.importance)
        output.loc[output.importance < 0, 'cumulative'] = None
        output['share'] = output.cumulative / output.query('importance > 0').importance.sum()
        output['selected'] = output.share <= threshold
        print('Done.')
        return output

    def _illustrator(self, features):
        print('\tPlot results...', end=' ', flush=True)
        utils = self.Utils()
        figure, ax = self.plt.subplots(figsize=(10, 100), dpi=350)
        self.sns.barplot(data=features, y='feature', x='importance', palette="RdBu", hue='selected')
        self.sns.set_style(style='darkgrid')
        ax.set_title('Permutation feature importance')
        directory = utils.get_full_path('data/dataset/reporting')
        path = self.path.join(directory, 'feature_importance.png')
        self.Path(directory).mkdir(parents=True, exist_ok=True)
        figure.savefig(path, dpi=350, bbox_inches='tight')
        self.plt.close()
        print('Done.')

    def _select_features(self, features):
        print('\tSelect features...', end=' ', flush=True)
        utils = self.Utils()
        selected_features = features[features.selected].feature.tolist()
        directory = utils.get_full_path('data/dataset/processed/operations')
        path = self.path.join(directory, 'features.txt')
        self.Path(directory).mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as f:
            f.write('\n'.join(selected_features))
        print('Done.')


if __name__ == '__main__':
    FeatureSelection().feature_selection()
