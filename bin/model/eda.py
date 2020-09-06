# Generate feature report
print('\nGenerating feature report...', end=' ', flush=True)
report = self.ProfileReport(data, title="Pandas Profiling Report", pool_size=16, minimal=True)
report_directory = utils.get_full_path('data/eda')
self.Path(report_directory).mkdir(parents=True, exist_ok=True)
report_filename = self.path.join(report_directory, 'profiling_report.html')
report.to_file(output_file=report_filename)
helper.pause(1)
print('Done.')