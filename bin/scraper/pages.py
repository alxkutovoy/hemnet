class Pages(object):

    import pandas as pd

    from tqdm import tqdm

    from bin.scraper.operations.captcha import Captcha
    from bin.scraper.operations.engine import Engine
    from utils.helper import Helper
    from utils.var import File, Dir
    from utils.io import IO

    def dataset(self):
        helper, io, engine = self.Helper(), self.IO(), self.Engine()
        helper.metadata_synch()  # Synch sitemap metadata
        directory = self.Dir.PAGES
        sitemap = io.read_pq(self.File.SITEMAP)
        print('\nDownload property pages:',
              f'\nThere are {len(sitemap)} property pages. '
              f'{(sitemap.extract == True).sum()} of them are already extracted.')
        # Filer only relevant entries and check if there are any new pages to work with
        sitemap = sitemap.query('extract == False')
        if len(sitemap) == 0:
            print('There are no new pages pages to extract data from.')
            return
        # Download pages
        print(f'\nDownloading {len(sitemap)} pages...')
        months = list(set(['-'.join(x.split('-')[0:2]) for x in sitemap.create_date.unique()]))
        months.sort()
        # Launch progress bar
        io.pause(2)
        bar = self.tqdm(total=len(sitemap), bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        io.pause()
        # Iterate over months
        for month in months:
            sitemap_subset = sitemap.query("create_date.str.contains(@month)", engine='python')
            self._extract(sitemap=sitemap_subset, pages_directory=directory, bar=bar, month=month)
        # End
        bar.close()
        io.pause(2)  # Prevents issues with the layout of update messages im terminal
        print('Pages have been successfully downloaded.')
        engine.shutdown_engine()  # Close Safari

    def _extract(self, sitemap, pages_directory, bar, month):
        helper, engine, io = self.Helper(), self.Engine(), self.IO()
        driver = engine.initiate_engine()
        data, total_pages, absent_pages = [], len(sitemap), 0
        suffix = '_'.join(month.split('-')[0:2])  # Prefix: YYYY_MM
        name = f'pages_{suffix}.parquet'
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
            if len(data) > 0 and len(data) % 100 == 0:  # Save progress every 100 pages
                self._save_pages(data, name, sitemap)
                data = []  # Reset buffer
        self._save_pages(data, name, sitemap) if len(data) > 0 else None

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
        self.pd.options.mode.chained_assignment = None
        sitemap['extract'][index] = True
        sitemap['extract_ts'][index] = io.now()

    def _save_pages(self, data, name, sitemap):
        helper, io = self.Helper(), self.IO()
        # Convert list into a dataframe and add extra columns
        df = self.pd.DataFrame(data)
        df.columns = ['url', 'page']
        df.insert(0, 'add_ts', io.now())
        # Update sitemap dataset
        old = io.read_pq(self.File.SITEMAP)
        old.update(sitemap)
        io.save_pq(data=old, path=self.File.SITEMAP)
        # Save into *.parquet file
        helper.update_pq(data=df, path=io.path_join(self.Dir.PAGES, name), dedup=['url'], com=False)


if __name__ == '__main__':
    Pages().dataset()
