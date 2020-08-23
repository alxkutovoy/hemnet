class PublicTransport(object):

    import json
    import requests
    import pandas as pd

    from os import path

    from bin.helpers.helper import Helper

    def get_items(self):
        helper = self.Helper()
        print('\nRetrieving Stockholm public transport data...')
        api_key = helper.get_api_key('traficlab', 'sl_stops_and_lines')
        url = 'https://api.sl.se/api2/LineData.json?model=stop&key=' + api_key
        request = self.requests.get(url)
        data = request.json()
        print('\nData received. Saving...')
        # Save files
        directory = "../../../resource"
        json_path = self.path.join(directory, "public_transport.json")
        with open(json_path, 'w') as f:
            self.json.dump(data, f)
        with open(json_path) as json_file:
            data = self.json.load(json_file)
        items = data['ResponseData']['Result']
        df = self.pd.DataFrame(items)
        helper.save_as_parquet(df, directory, "public_transport.parquet", "StopPointNumber")
        print("\nCompleted.")


if __name__ == '__main__':
    PublicTransport().get_items()
