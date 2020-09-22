class FeatureSelection(object):

    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    import seaborn as sns

    from lightgbm.sklearn import LGBMRegressor

    from utils.data import Data
    from utils.var import File
    from utils.io import IO

    def feature_selection(self, threshold=0.99, criterion='neg_root_mean_squared_error'):
        from sklearn.inspection import permutation_importance
        print('\nSelect features:')
        # Get data
        data = self.Data().processed()
        x_train, x_val, y_train, y_val = data['x_train'], data['x_val'], data['y_train'], data['y_val']
        # Define classifier and fit data
        print('\n\tDefine classifier and fit data...', end=' ', flush=True)
        pipe = self.LGBMRegressor()
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
        io = self.IO()
        figure, ax = self.plt.subplots(figsize=(10, 100), dpi=350)
        self.sns.barplot(data=features, y='feature', x='importance', palette="RdBu", hue='selected')
        self.sns.set_style(style='darkgrid')
        ax.set_title('Permutation feature importance')
        path = self.File.FEATURE_IMPORTANCE_REPORT
        io.make_dir(io.dir(path))
        figure.savefig(path, dpi=350, bbox_inches='tight')
        self.plt.close()
        print('Done.')

    def _select_features(self, features):
        print('\tSelect features...', end=' ', flush=True)
        io = self.IO()
        selected_features = features[features.selected].feature.tolist()
        path = self.File.SELECTED_FEATURES
        io.make_dir(io.dir(path))
        with open(path, 'w') as f:
            f.write('\n'.join(selected_features))
        print('Done.')


if __name__ == '__main__':
    FeatureSelection().feature_selection()
