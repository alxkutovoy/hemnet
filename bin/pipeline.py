class DataPipeline(object):
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
        # Initiate
        sitemap = self.Sitemap()
        pages = self.Pages()
        content = self.Content()
        # Run
        sitemap.dataset()
        pages.dataset()
        content.dataset()

    # Enrichment layer

    def run_features(self):
        # Initiate
        data = self.Data()
        entities = self.Entities()
        transport = self.Transport()
        destinations = self.Destinations()
        enrichment = self.Enrichment()
        # Run
        data.generate_dataset()
        entities.entities()
        transport.transport()
        destinations.destinations()
        enrichment.data()

    # Modeling
    
    def run_model(self):
        # Initiate
        eda = self.EDA()
        preprocessing = self.Preprocessing()
        feature_selection = self.FeatureSelection()
        hyperparameters = self.Hyperparameters()
        train = self.Train()
        # Run
        eda.report()
        preprocessing.preprocessing()
        feature_selection.feature_selection()
        hyperparameters.hyperparameters_tuning()
        train.train()


if __name__ == '__main__':
    DataPipeline().run_pipeline()
