class Clustering(object):

    import pandas as pd
    import matplotlib.pyplot as plt
    import joblib
    import seaborn as sns

    from bin.queries.operations.kmeans import KMeans

    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO
    from utils.geo import Geo

    def clustering(self, n_clusters=100, retrain=None):
        print('\nEnrich dataset with clustering based on geographical coordinates of properties:')
        helper, io, geo = self.Helper(), self.IO(), self.Geo()
        # Get raw property data
        data = helper.remove_duplicates(original_path=self.File.SUBSET,
                                        target_path=self.File.CLUSTERS,
                                        select=['url', 'latitude', 'longitude'],
                                        dedup=['url'])
        # Check if anything to work on
        if len(data) == 0:
            print('There are no new properties to work on.')
            return
        # Assign clusters
        if retrain or not io.exists(self.File.KMEANS):
            data = data[['latitude', 'longitude', 'url']]
            self.KMeans().kmeans(data=data, n_clusters=n_clusters)
        model = self.joblib.load(open(self.File.KMEANS, 'rb'))
        data[['cluster']] = model.predict(data[['latitude', 'longitude']])
        self._plot_clusters(data[['latitude', 'longitude', 'cluster']], path=self.File.PROPERTY_CLUSTERING_REPORT)
        # Save
        helper.update_pq(data=data, path=self.File.CLUSTERS, dedup=['url'])
        print("\nCompleted.")

    def request_clustering(self, request):
        self.pd.set_option('mode.chained_assignment', None)
        original_columns = list(request.columns)
        model = self.joblib.load(open(self.File.KMEANS, 'rb'))
        request[['cluster']] = model.predict(request)
        return request.drop(columns=original_columns, axis=1)

    def _plot_clusters(self, data, path):
        io = self.IO()
        directory, name = io.dir_and_base(path)
        self.sns.set()
        plot = self.sns.scatterplot(x='latitude', y='longitude', data=data,
                                    hue=data.cluster.tolist(), legend=False, palette='muted')
        self.plt.title('Property Clustering')
        self.plt.xlabel('Latitude')
        self.plt.ylabel('Longitude')
        io.make_dir(directory)
        plot.figure.savefig(path, dpi=900)
        self.plt.close()


if __name__ == '__main__':
    Clustering().clustering(retrain=True)
