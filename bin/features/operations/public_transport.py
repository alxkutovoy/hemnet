class PublicTransport(object):

    import json
    import os
    import pandas as pd
    import requests

    from os import path
    from pathlib import Path

    from bin.helpers.helper import Helper

    def transport(self):
        print('\nExtract public transportation data:')
        helper = self.Helper()
        directory = "../../../resource/public_transport"
        stops_name, routes_name = "stops.json", "routes.json"
        # Get raw data
        self._get_data(directory=directory, name=stops_name, key='jour', update=False)
        self._get_data(directory=directory, name=routes_name, key='stop', update=False)
        # Get data frames from json
        print('\nConvert *.json into pandas data frame...')
        stops = data = self._json_to_df(directory, stops_name)
        routes = self._json_to_df(directory, routes_name)
        # Combine data sets
        print('Combine data sets...')
        data['lines'] = stops.apply(lambda x: self._get_lines(routes, x.StopPointNumber), axis=1)
        helper.save_as_parquet(data, directory, "transport.parquet", "StopPointNumber")
        print("\nCompleted.")


    def _get_data(self, directory, name, key, update=False):
        upper_case, lower_case = name.split(".")[0].capitalize(), name.split(".")[0]
        print('\nRetrieve {} data:'.format(lower_case))
        # Don't update if exists and no order to update
        file_path = self.path.join(directory, name)
        if self.os.path.isfile(file_path) and not update:
            print('{} data is already downloaded.'.format(upper_case))
        else:
            data = self._process_request(key)
            print('{} data was retrieved. Saving...'.format(upper_case))
            # Save file
            self._save_json(data, directory, name)
            print('{} data was saved.'.format(upper_case))

    def _process_request(self, key):
        helper = self.Helper()
        api_key = helper.get_api_key('traficlab', 'sl_stops_and_lines')
        url = "https://api.sl.se/api2/LineData.json?model={}&key={}".format(key, api_key)
        request = self.requests.get(url)
        data = request.json()
        return data

    def _save_json(self, data, directory, name):
        self.Path(directory).mkdir(parents=True, exist_ok=True)
        json_path = self.path.join(directory, name)
        with open(json_path, 'w') as f:
            self.json.dump(data, f)

    def _json_to_df(self, directory, name):
        path = self.path.join(directory, name)
        with open(path) as json_file:
            data = self.json.load(json_file)
        df = self.pd.DataFrame(data['ResponseData']['Result'])
        return df

    def _get_lines(self, routes, stop_number):
        stop_number = str(stop_number)
        if len(stop_number) == 4:
            stop_number = '0' + stop_number
        return list(set(list(routes.query('JourneyPatternPointNumber == @stop_number').LineNumber)))  # set = dedup


if __name__ == '__main__':
    PublicTransport().transport()
