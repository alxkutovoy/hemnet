class DataPipeline(object):

    from bin.scraper.sitemap import Sitemap
    from bin.scraper.pages import Pages
    from bin.scraper.content import Content

    from bin.features.data import Data
    from bin.features.entities import Entities
    from bin.features.transport import Transport
    from bin.features.destinations import Destinations
    from bin.features.enrichment import Enrichment

    def run_pipeline(self):
        self.run_scraper()
        self.run_features()

    def run_scraper(self):
        sitemap = self.Sitemap()
        pages = self.Pages()
        content = self.Content()
        sitemap.dataset()
        pages.dataset()
        content.dataset()

    def run_features(self):
        data = self.Data()
        entities = self.Entities()
        transport = self.Transport()
        destinations = self.Destinations()
        enrichment = self.Enrichment()
        data.generate_dataset()
        entities.entities()
        transport.transport()
        destinations.destinations()
        enrichment.data()


if __name__ == '__main__':
    DataPipeline().run_pipeline()
