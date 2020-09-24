class Data(object):

    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO

    def generate_dataset(self):
        print('\nGenerate a sample from the dataset:')
        helper, io = self.Helper(), self.IO()
        content = io.read_pq(self.File.CONTENT)
        # Filter relevant entries
        print('\nSample relevant entries...')
        data = content.query('urban_area == "Stockholm tätort" & property_type == "Lägenhet"')  # Configurable filters
        # Save
        helper.update_pq(data=data, path=self.File.SUBSET, dedup=['url'])
        print("\nCompleted.")


if __name__ == '__main__':
    Data().generate_dataset()
