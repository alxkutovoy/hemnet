class Pipeline(object):

    from bin.scraper.sitemap import Sitemap
    from bin.scraper.pages import Pages
    from bin.scraper.content import Content

    from bin.queries.data import Data
    from bin.queries.layers.entities import Entities
    from bin.queries.layers.address import Address
    from bin.queries.layers.transport import Transport
    from bin.queries.layers.destinations import Destinations
    from bin.queries.layers.clustering import Clustering
    from bin.queries.enrichment import Enrichment

    from bin.model.eda import EDA
    from bin.model.preprocessing import Preprocessing
    from bin.model.feature_selection import FeatureSelection
    from bin.model.hyperparameters import Hyperparameters
    from bin.model.train import Train

    def run_pipeline(self):
        self.run_scraper()
        self.run_queries()
        self.run_model()

    # Data scraping

    def run_scraper(self):
        self.Sitemap().dataset()
        self.Pages().dataset()
        self.Content().dataset()

    # Enrichment layer

    def run_queries(self):
        self.Data().generate_dataset()
        self.Entities().entities()
        self.Address().address()
        self.Transport().transport()
        self.Destinations().destinations()
        self.Clustering().clustering(retrain=True)
        self.Enrichment().data()

    # Modeling

    def run_model(self):
        self.EDA().report()
        self.Preprocessing().preprocessing()
        self.FeatureSelection().feature_selection()
        self.Hyperparameters().hyperparameters_tuning()
        self.Train().train()


if __name__ == '__main__':
    Pipeline().run_scraper()
