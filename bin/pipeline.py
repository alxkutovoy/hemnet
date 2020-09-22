class Pipeline(object):

    from bin.scraper.sitemap import Sitemap
    from bin.scraper.pages import Pages
    from bin.scraper.content import Content

    from bin.features.data import Data
    from bin.features.entities import Entities
    from bin.features.transport import Transport
    from bin.features.destinations import Destinations
    from bin.features.enrichment import Enrichment

    from bin.model.eda import EDA
    from bin.model.preprocessing import Preprocessing
    from bin.model.feature_selection import FeatureSelection
    from bin.model.hyperparameters import Hyperparameters
    from bin.model.train import Train

    def run_pipeline(self):
        self.run_scraper()
        self.run_features()
        self.run_model()

    # Data scraping

    def run_scraper(self):
        self.Sitemap().dataset()
        self.Pages().dataset()
        self.Content().dataset()

    # Enrichment layer

    def run_features(self):
        self.Data().generate_dataset()
        self.Entities().entities()
        self.Transport().transport()
        self.Destinations().destinations()
        self.Enrichment().data()

    # Modeling

    def run_model(self):
        self.EDA().report()
        self.Preprocessing().preprocessing()
        self.FeatureSelection().feature_selection()
        self.Hyperparameters().hyperparameters_tuning()
        self.Train().train()


if __name__ == '__main__':
    Pipeline().run_pipeline()
