class PublicTransport(object):

    import json
    import pandas as pd
    import requests

    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO
    from utils.api import API

    def transport(self):
        print('\nExtract public transportation data:')
        helper, io = self.Helper(), self.IO()
        # Get raw data
        self._get_data(path=self.File.SL_STOPS, key='stop', update=False)
        self._get_data(path=self.File.SL_ROUTES, key='jour', update=False)
        # Get data frames from json
        print('\nConvert *.json into pandas data frame...')
        stops = self._json_to_df(self.File.SL_STOPS)
        routes = self._json_to_df(self.File.SL_ROUTES)
        data = stops
        # Combine data sets
        print('Combine data sets...')
        data['lines'] = stops.apply(lambda x: self._get_lines(routes, x.StopPointNumber), axis=1)
        helper.update_pq(data=data, path=self.File.SL_PROCESSED, dedup=["StopPointNumber"])
        print("\nCompleted.")

    def _get_data(self, path, key, update=False):
        io = self.IO()
        name = io.base(path)
        upper_case, lower_case = name.split(".")[0].capitalize(), name.split(".")[0]
        print(f'\nRetrieve {lower_case} data:')
        # Don't update if exists and no order to update
        if io.exists(path) and not update:
            print(f'{upper_case} data is already downloaded.')
        else:
            data = self._process_request(key)
            print(f'{upper_case} data was retrieved. Saving...')
            # Save file
            io.save_json(data=data, path=path)
            print(f'{upper_case} data was saved.')

    def _process_request(self, key):
        api = self.API()
        api_key = api.key('traficlab', 'sl_stops_and_lines_key')
        url = f"https://api.sl.se/api2/LineData.json?model={key}&key={api_key}"
        request = self.requests.get(url)
        data = request.json()
        return data

    def _json_to_df(self, path):
        with open(path) as json_file:
            data = self.json.load(json_file)
        df = self.pd.DataFrame(data['ResponseData']['Result'])
        return df

    def _get_lines(self, routes, stop_number):
        stop_number = str(stop_number)
        if len(stop_number) == 4:
            stop_number = '0' + stop_number  # NB! It is actually used in a query below
        return list(set(list(routes.query('JourneyPatternPointNumber == @stop_number').LineNumber)))  # set = dedup


if __name__ == '__main__':
    PublicTransport().transport()
