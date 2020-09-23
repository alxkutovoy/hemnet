class KMeans(object):

    import pandas as pd

    from sklearn.cluster import KMeans

    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO
    from utils.geo import Geo

    def kmeans(self, data, n_clusters=100):
        helper, io, geo = self.Helper(), self.IO(), self.Geo()
        print(f"\nBuilding a new KMeans model for {n_clusters} clusters...", end=' ', flush=True)
        n_clusters = len(data) if n_clusters > len(data) else n_clusters  # n_clusers must be >= n_samples
        data[['lat', 'lng']] = self.pd.DataFrame(data.coordinates.tolist(), index=data.index)
        kmeans = self.KMeans(n_clusters=n_clusters, init='k-means++')
        io.save_pkl(model=kmeans, path=self.File.KMEANS)
        print('Done.')
