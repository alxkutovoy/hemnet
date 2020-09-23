class Clustering(object):

    import matplotlib.pyplot as plt
    import pandas as pd
    import pickle
    import seaborn as sns

    from bin.queries.operations.kmeans import KMeans

    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO
    from utils.geo import Geo

    def clustering(self, n_clusters=100, request=None, retrain=None):
        print('\nEnrich dataset with clustering based on geographical coordinates of properties:')
        helper, io, geo = self.Helper(), self.IO(), self.Geo()
        # Get raw property data
        if not request:
            data = helper.remove_duplicates(self.File.SUBSET, self.File.CLUSTERS, ['url', 'coordinates'], ['url'])
        else:
            data = request
        # Check if anything to work on
        if len(data) == 0:
            print('There are no new properties to work on.')
            return
        # Assign clusters
        if not request and (retrain or not io.exists(self.File.KMEANS)):
            data[['lat', 'lng']] = self.pd.DataFrame(data.coordinates.tolist(), index=data.index)
            self.KMeans().kmeans(data=data, n_clusters=n_clusters)
        model = self.pickle.load(open(self.File.KMEANS, 'rb'))
        data[['cluster']] = model.fit_predict(data[['lat', 'lng']])
        if not request:
            self._plot_clusters(data[['lat', 'lng', 'cluster']], path=self.File.PROPERTY_CLUSTERING_REPORT)
        # Save
        if not request:
            helper.update_pq(data=data, path=self.File.CLUSTERS, dedup=['url'])
        else:
            return data
        print("\nCompleted.")

    def _plot_clusters(self, data, path):
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


if __name__ == '__main__':
    Clustering().clustering(retrain=True)
