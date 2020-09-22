class Pages(object):

    import pandas as pd

    from tqdm import tqdm

    from bin.scraper.operations.captcha import Captcha
    from bin.scraper.operations.engine import Engine
    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO

    def dataset(self):
        helper, io, engine = self.Helper(), self.IO(), self.Engine()
        helper.metadata_synch()  # Synch sitemap metadata
        directory, name = io.dir_and_base(self.File.PAGES)
        sitemap = io.read_pq(self.File.SITEMAP)
        total, extracted = len(sitemap), (sitemap.extract == True).sum()
        print('\nDownload property pages:',
              f'\nThere are {total} property pages. {extracted} of them are already extracted.')
        # Filer only relevant entries and check if there are any new pages to work with
        sitemap = sitemap.query('extract == False')
        if len(sitemap) == 0:
            print('There are no new pages pages to extract data from.')
            return
        # Download pages
        print('\nDownloading pages...')
        data = self._extract(sitemap, directory)
        # Convert list into a dataframe and add extra columns
        df = self.pd.DataFrame(data)
        df.columns = ['url', 'page']
        df.insert(0, 'add_ts', io.now())
        # Info
        new = (sitemap.extract == True).sum()
        print(f'Adding {new} new pages. {new + extracted} pages in total. {extracted} were already extracted.')
        helper.logger(directory, name, new + extracted, new, extracted)
        # Update sitemap dataset
        old = io.read_pq(self.File.SITEMAP)
        old.update(sitemap)
        io.save_pq(data=old, path=self.File.SITEMAP)
        print('\nThe sitemap dataset has been successfully updated.')
        # Save into *.parquet file
        helper.update_pq(data=df, path=self.File.PAGES, dedup=['url'])
        # End
        print('\nAll pages have been successfully downloaded.')
        engine.shutdown_engine()  # Close Safari

    def _extract(self, sitemap, pages_directory):
        helper, engine, io = self.Helper(), self.Engine(), self.IO()
        driver = engine.initiate_engine()
        io.pause(2)
        data, total_pages, absent_pages = [], len(sitemap), 0
        bar = self.tqdm(total=total_pages, bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        io.pause()
        for index in sitemap.index:
            entry = sitemap.loc[[index]]
            url = entry.url.item()
            # Extract data
            try:
                data += [[url, self._download(url, driver)]]
                # Update extract values
                self._mark(index, sitemap)
            except Exception as e:
                driver = engine.initiate_engine()  # Restart engine
                io.pause(5)
                helper.errors(directory=pages_directory, index=index, url=url, error=e)
            bar.update(1)
        bar.close()
        io.pause(2)  # Prevents issues with the layout of update messages im terminal
        return data

    def _download(self, url, driver):
        io, captcha = self.IO(), self.Captcha()
        driver.get(url)
        io.pause()
        content = driver.page_source
        # Fix captcha
        bad_flag = 'captcha'
        if bad_flag in content:
            content = captcha.anticaptcha(content, url, driver)  # Bypass captcha
        # Record data
        if content is not None:
            return content

    def _mark(self, index, sitemap):
        io = self.IO()
        sitemap['extract'][index] = True
        sitemap['extract_ts'][index] = io.now()


if __name__ == '__main__':
    Pages().dataset()
