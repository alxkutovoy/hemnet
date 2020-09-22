class Content(object):

    import pandas as pd

    from tqdm import tqdm

    from bin.scraper.operations.parser import Parser
    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO

    def dataset(self):
        helper, io = self.Helper(), self.IO()
        helper.metadata_synch()  # Synchronise sitemap metadata
        sitemap = io.read_pq(self.File.SITEMAP)
        total, extracted, parsed = len(sitemap), (sitemap.extract == True).sum(), (sitemap.parse == True).sum()
        print('\nExtract content from pages:',
              f'\nThere are {total} property pages. {extracted} of them are extracted and {parsed} are parsed.')
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
        df.insert(0, 'add_ts', io.now())
        # Update sitemap dataset
        old = io.read_pq(self.File.SITEMAP)
        old.update(sitemap)
        io.save_pq(data=old, path=self.File.SITEMAP)
        # Save into *.parquet file
        helper.update_pq(data=df, path=self.File.CONTENT, dedup=['url'])  # Save data into a *.parquet
        print('\nThe sitemap dataset has been successfully updated.')

    def _extract(self, sitemap):
        io = self.IO()
        # Load pages.parquet
        pages = io.read_pq(self.File.PAGES)
        io.pause(2)
        data, total_pages, parsed_pages = [], len(sitemap), (sitemap.parse == True).sum()
        bar = self.tqdm(total=total_pages-parsed_pages, bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        io.pause()
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
            sitemap['parse_ts'][index] = io.now()
        bar.close()
        io.pause()  # Prevents issues with the layout of update messages im terminal
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
