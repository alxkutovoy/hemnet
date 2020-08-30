class Content(object):

    import pandas as pd

    from datetime import datetime
    from tqdm import tqdm

    from bin.helpers.helper import Helper
    from bin.scraper.operations.parser import Parser
    from utils.files import Utils

    def dataset(self):
        utils, helper = self.Utils(), self.Helper()
        helper.metadata_synch()  # Synch sitemap metadata
        sitemap_path = utils.get_full_path('data/sitemap/sitemap.parquet')
        content_directory = utils.get_full_path('data/content')
        sitemap = self.pd.read_parquet(sitemap_path, engine="fastparquet")
        total, extracted, parsed = len(sitemap), (sitemap.extract == True).sum(), (sitemap.parse == True).sum()
        print('\nExtract content from pages:',
              '\nThere are', total, 'property pages.', extracted, 'of them are extracted and', parsed, 'are parsed.')
        # Filter only relevant entries and check if there are any new pages to work with
        sitemap = sitemap.query('extract == True and parse == False')
        if len(sitemap) == 0:
            print('There are no new pages pages to extract data from.')
            return
        # Extract data
        print('\nExtracting content...')
        data = self._extract(sitemap)
        # Convert list into a dataframe and add extra columns
        df = self.pd.DataFrame(data)
        if len(df) == 0:
            print('\nNo pages were parsed.')
            return
        df.columns = self._columns()
        df.insert(0, 'add_ts', self.datetime.now().replace(microsecond=0))
        # Update sitemap dataset
        old = self.pd.read_parquet(sitemap_path, engine="fastparquet")
        old.update(sitemap)
        old.to_parquet(path=sitemap_path, compression='gzip', engine="fastparquet")
        # Save into *.parquet file
        file_name, dedup_column = 'content.parquet', 'url'
        helper.save_as_parquet(df, content_directory, file_name, dedup_column)  # Save data into a *.parquet
        print('\nThe sitemap dataset has been successfully updated.')

    def _extract(self, sitemap):
        utils, helper = self.Utils(), self.Helper()
        # Load pages.parquet
        pages_path = utils.get_full_path('data/pages/pages.parquet')
        pages = self.pd.read_parquet(pages_path, engine="fastparquet")
        helper.pause(2)
        data, total_pages, parsed_pages = [], len(sitemap), (sitemap.parse == True).sum()
        bar = self.tqdm(total=total_pages-parsed_pages, bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        helper.pause()
        for index in sitemap.index:
            entry = sitemap.loc[[index]]
            url = entry.url.item()
            content = pages.loc[pages.url == url].page.item()
            # Parse data
            data += [self._parser(content, url)]
            bar.update(1)
            # Update extract values
            self.pd.set_option('mode.chained_assignment', None)  # Yeah, I know
            sitemap['parse'][index] = True
            sitemap['parse_ts'][index] = self.datetime.now().replace(microsecond=0)
        bar.close()
        helper.pause()  # Prevents issues with the layout of update messages im terminal
        return data

    def _columns(self):
        return [
            "hemnet_ts", "property_id", "sold_property_id", "sold_at_date", "property_type", "ownership_type",
            "country", "region", "urban_area", "municipality", "location", "city", "district", "neighborhood",
            "address", "street", "coordinates", "start_price", "end_price", "price_sqm", "rums", "area", "utils",
            "build_year", "broker_full_name", "broker_agency_id", "broker_agency", "broker_phone", "broker_email", "url"
        ]

    def _parser(self, content, url):
        parser = self.Parser()
        return [parser.get_ts(content),
                parser.get_property_id(content),
                parser.get_sold_property_id(content),
                parser.get_sold_at_date(content),
                parser.get_property_type(content),
                parser.get_ownership_type(content),
                parser.get_country(content),
                parser.get_region(content),
                parser.get_urban_area(content),
                parser.get_municipality(content),
                parser.get_location(content),
                parser.get_city(content),
                parser.get_district(content),
                parser.get_neighborhood(content),
                parser.get_address(content),
                parser.get_street(content),
                parser.get_coordinates(content),
                parser.get_start_price(content),
                parser.get_end_price(content),
                parser.get_price_sqm(content),
                parser.get_rums(content),
                parser.get_area(content),
                parser.get_utils(content),
                parser.get_build_year(content),
                parser.get_broker_full_name(content),
                parser.get_broker_agency_id(content),
                parser.get_broker_agency(content),
                parser.get_broker_phone(content),
                parser.get_broker_email(content),
                url]


if __name__ == '__main__':
    Content().dataset()
