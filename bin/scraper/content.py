class Content(object):

    import pandas as pd

    from datetime import datetime
    from tqdm import tqdm

    from bin.scraper.operations.helper import Helper
    from bin.scraper.operations.parser import Parser

    def dataset(self):
        helper = self.Helper()
        helper.metadata_synch()  # Synch sitemap metadata
        sitemap_path, content_directory = '../../data/sitemap/sitemap.parquet', '../../data/content/'
        sitemap = self.pd.read_parquet(sitemap_path)
        total, extracted, parsed = len(sitemap), (sitemap.extract == True).sum(), (sitemap.parse == True).sum()
        print('\nExtract content from pages:',
              '\nThere are', total, 'property pages.', extracted, 'of them are extracted and', parsed, 'are parsed.')
        # Filter only relevant entries and check if there are any new pages to work with
        sitemap = sitemap.query('extract == True and parse == False')
        if len(sitemap) == 0:
            print('\nThere are no new pages pages to extract data from.')
            return
        # Extract data
        print('\nExtracting content...')
        data = self._extract(sitemap)
        # Convert list into a dataframe and add extra columns
        df = self.pd.DataFrame(data)
        if len(df) == 0:
            print('\nNo pages were parsed.')
            return
        df.columns = ['hemnet_ts', 'municipality', 'neighborhood',  'address', 'proptype',  'owntype', 'coordinates',
                      'startprice', 'endprice', 'pricesqm', 'rum', 'area',  'utils', 'buildyear', 'brokerfullname',
                      'brokeragency', 'brokerphone', 'url']
        df.insert(0, 'add_ts', self.datetime.now().replace(microsecond=0))
        # Update sitemap dataset
        old = self.pd.read_parquet(sitemap_path)
        old.update(sitemap)
        old.to_parquet(sitemap_path, compression='gzip')
        # Save into *.parquet file
        file_name, dedup_column = 'content.parquet', 'url'
        helper.save_as_parquet(df, content_directory, file_name, dedup_column)  # Save data into a *.parquet
        print('\nThe sitemap dataset has been successfully updated.')

    def _extract(self, sitemap):
        helper = self.Helper()
        # Load pages.parquet
        pages = self.pd.read_parquet('../../data/pages/pages.parquet')
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

    def _parser(self, content, url):
        parser = self.Parser()
        return [parser.get_ts(content),
                parser.get_municipality(content),
                parser.get_neighborhood(content),
                parser.get_address(content),
                parser.get_proptype(content),
                parser.get_owntype(content),
                parser.get_coordinates(content),
                parser.get_startprice(content),
                parser.get_endprice(content),
                parser.get_pricesqm(content),
                parser.get_rum(content),
                parser.get_area(content),
                parser.get_utils(content),
                parser.get_buildyear(content),
                parser.get_brokerfullname(content),
                parser.get_brokeragency(content),
                parser.get_brokerphone(content),
                url]


if __name__ == '__main__':
    Content().dataset()
d