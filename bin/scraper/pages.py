class Pages(object):

    import pandas as pd

    from datetime import datetime
    from tqdm import tqdm

    from bin.scraper.operations.captcha import Captcha
    from bin.scraper.operations.engine import Engine
    from bin.scraper.operations.helper import Helper

    def dataset(self):
        engine, helper = self.Engine(), self.Helper()
        helper.metadata_synch()  # Synch sitemap metadata
        sitemap_path, pages_directory = '../../data/sitemap/sitemap.parquet', '../../data/pages'
        sitemap = self.pd.read_parquet(sitemap_path)
        total, extracted = len(sitemap), (sitemap.extract == True).sum()
        print('\nDownload property pages:',
              '\nThere are', total, 'property pages.', extracted, 'of them are already extracted.')
        # Filer only relevant entries and check if there are any new pages to work with
        sitemap = sitemap.query('extract == False')
        if len(sitemap) == 0:
            print('\nThere are no new pages pages to extract data from.')
            return
        # Download pages
        print('\nDownloading pages...')
        data = self._extract(sitemap, pages_directory)
        # Convert list into a dataframe and add extra columns
        df = self.pd.DataFrame(data)
        df.columns = ['url', 'page']
        df.insert(0, 'add_ts', self.datetime.now().replace(microsecond=0))
        # Info
        new = (sitemap.extract == True).sum()
        print('Adding', new, 'new pages.', new + extracted, 'pages in total.',  extracted, 'were already extracted.')
        helper.logger(pages_directory, new + extracted, new, extracted)
        # Update sitemap dataset
        old = self.pd.read_parquet(sitemap_path)
        old.update(sitemap)
        old.to_parquet(sitemap_path, compression='gzip')
        print('\nThe sitemap dataset has been successfully updated.')
        # Save into *.parquet file
        helper.save_as_parquet(data=df, directory=pages_directory, file_name='pages.parquet', dedup_column='url')
        # End
        print('\nAll pages have been successfully downloaded.')
        engine.shutdown_engine()  # Close Safari

    def _extract(self, sitemap, pages_directory):
        engine, helper = self.Engine(), self.Helper()
        driver = engine.initiate_engine()
        helper.pause(2)
        data, total_pages, absent_pages = [], len(sitemap), 0
        bar = self.tqdm(total=total_pages, bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        helper.pause()
        for index in sitemap.index:
            entry = sitemap.loc[[index]]
            url = entry.url.item()
            # Extract data
            try:
                data += [[url, self._download(url, driver)]]
                # Update extract values
                self._update_metadata(index, sitemap)
            except Exception as e:
                driver = engine.initiate_engine()  # Restart engine
                helper.pause(5)
                helper.errors(directory=pages_directory, index=index, url=url, error=e)
            bar.update(1)
        bar.close()
        helper.pause(2)  # Prevents issues with the layout of update messages im terminal
        return data

    def _download(self, url, driver):
        helper, captcha = self.Helper(), self.Captcha()
        driver.get(url)
        helper.pause()
        content = driver.page_source
        # Fix captcha
        bad_flag = 'captcha'
        if bad_flag in content:
            content = captcha.anticaptcha(content, url, driver)  # Bypass captcha
        # Record data
        if content is not None:
            return content

    def _update_metadata(self, index, sitemap):
        sitemap['extract'][index] = True
        sitemap['extract_ts'][index] = self.datetime.now().replace(microsecond=0)


if __name__ == '__main__':
    Pages().dataset()
