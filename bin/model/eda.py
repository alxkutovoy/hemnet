class EDA(object):

    import pandas as pd

    from os import path
    from pandas_profiling import ProfileReport
    from pathlib import Path

    from utils.files import Utils
    from bin.helpers.helper import Helper

    def report(self, data=None, directory=None, name=None, comments=True, sample=1000):
        helper, utils = self.Helper(), self.Utils()
        if data is None:
            default_data_path = utils.get_full_path('data/dataset/enriched/data.parquet')
            if helper.exists(default_data_path):
                data = self.pd.read_parquet(default_data_path, engine="fastparquet")
            else:
                print('To generate a profile report, kindly provide a valid dataset.')
                return
        if comments:
            print('\nGenerate profile report...')
        sample = len(data) if len(data) > sample else sample
        directory = utils.get_full_path('data/eda') if directory is None else directory
        name = 'data' if name is None else name
        file_name = name + '_profiling_report.html'
        report = self.ProfileReport(data, title=file_name.split('.')[0], pool_size=sample, minimal=True)
        report_directory = utils.get_full_path('data/eda')
        self.Path(report_directory).mkdir(parents=True, exist_ok=True)
        report_path = self.path.join(directory, file_name)
        report.to_file(output_file=report_path)
        helper.pause(1)
        if comments:
            print('Report generation was completed.')


if __name__ == '__main__':
    EDA().report()
