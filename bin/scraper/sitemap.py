class Sitemap(object):

    import pandas as pd
    import sys

    from bs4 import BeautifulSoup
    from tqdm import tqdm

    from bin.scraper.operations.captcha import Captcha
    from bin.scraper.operations.engine import Engine
    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO

    def dataset(self):
        helper, io, engine = self.Helper(), self.IO(), self.Engine()
        driver = engine.initiate_engine()
        print('\nExtract sitemap data:')
        sitemap_list = list(range(1, self._total_pages(driver) + 1))
        # Extract data from the website
        print('\nParsing sitemap...')
        bar = self.tqdm(total=len(sitemap_list), bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        data = []
        for i, page in enumerate(sitemap_list):
            data += self._parse(page, driver)
            bar.update(1)
        bar.close()
        # Convert list into a dataframe and add extra columns
        df = self.pd.DataFrame(data)
        df.columns = ['url', 'create_date', 'change_frequency', 'priority']
        self._add_meta_columns(df)
        # Save into *.parquet file
        helper.update_pq(data=df, path=self.File.SITEMAP, dedup=['url'])  # Save data into a *.parquet
        # End
        engine.shutdown_engine()  # Close Safari
        print('\nThe sitemap dataset has been successfully updated.')

    def _add_meta_columns(self, df):
        io = self.IO()
        df.insert(0, 'add_ts', io.now())
        df.insert(5, 'extract', False)  # Flag that will be changed to True once page is downloaded
        df.insert(6, 'extract_ts', '')  # Timestamp of page download
        df.insert(7, 'parse', False)  # Flag that will be changed to True once page is parsed
        df.insert(8, 'parse_ts', '')  # Timestamp of page parsed
        df['parse_ts'] = self.pd.to_datetime(df['parse_ts'])  # Ensure it's of the datetime64 type
        df['extract_ts'] = self.pd.to_datetime(df['extract_ts'])  # Ensure it's of the datetime64 type

    def _parse(self, index, driver):
        io, captcha, engine = self.IO(), self.Captcha(), self.Engine()
        url = 'https://www.hemnet.se/sitemap/sold_properties.xml?page=' + str(index)
        driver.get(url)
        content = driver.page_source
        if 'captcha' in str(content):
            io.pause()
            content = captcha.anticaptcha(content, url, driver)  # Bypass captcha
        # Return parsed content
        try:
            if content is not None:
                return self._decompose(content=content, input_type='sitemap')
            else:
                print(f'Something went wrong at the page {index}!')
        except Exception as e:
            method_name = self.sys._getframe().f_code.co_name
            print(f'[Error] Method: {method_name}. Error message: {str(e)}.')

    def _decompose(self, content, input_type='sitemap'):
        block = None
        if input_type == 'sitemap':
            block = 4
        if input_type == 'page_count':
            block = 2
        try:
            output, clean = self.BeautifulSoup(content, 'xml').find_all("span", {"class": "text"}), []
            for element in output:
                clean.append(element.text)
                output = clean
            # Remove redundant elements
            output = [e for e in output if e not in ('...', '')]
            # Group by blocks
            output = [output[k:k + block] for k in range(0, len(output), block)]
            return output
        except Exception as e:
            method_name = self.sys._getframe().f_code.co_name
            print(f'[Error] Method: {method_name}. Error message: {str(e)}.')

    def _total_pages(self, driver):
        io = self.IO()
        url, max_value = 'https://www.hemnet.se/sitemap.xml', 0
        driver.get(url)
        io.pause()
        data = self._decompose(driver.page_source, input_type='page_count')
        for element in data:
            if 'sold_properties' in element[0]:
                current_value = int(''.join(x for x in element[0] if x.isdigit()))  # Get only numbers from the string
                max_value = max(max_value, current_value)
        print(f'The total number of sitemap pages is {max_value}.')
        return max_value


if __name__ == '__main__':
    Sitemap().dataset()
