class EDA(object):

    from pandas_profiling import ProfileReport

    from utils.var import File, Dir
    from utils.io import IO

    def report(self, data=None, name=None, comments=True, sample=1000):
        io = self.IO()
        if data is None:
            if io.exists(self.File.ENRICHED_SUBSET):
                data = io.read_pq(self.File.ENRICHED_SUBSET)
            else:
                print('To generate a profile report, kindly provide a valid dataset.')
                return
        if comments:
            print('\nGenerate profile report...')
        sample = len(data) if len(data) < sample else sample
        name = io.base('data') if name is None else name + '_profiling_report.html'
        report = self.ProfileReport(data, title=name.split('.')[0], pool_size=sample, minimal=True)
        io.make_dir(self.Dir.EDA)
        report.to_file(output_file=io.path_join(self.Dir.EDA, name))
        io.pause(1)
        if comments:
            print('Report generation was completed.')


if __name__ == '__main__':
    EDA().report()
