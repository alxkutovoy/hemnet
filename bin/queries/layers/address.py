class Address(object):

    import pandas as pd

    from googlemaps import Client as GoogleMaps
    from tqdm import tqdm

    from utils.api import API
    from utils.helper import Helper
    from utils.var import File
    from utils.io import IO
    from utils.geo import Geo

    def address(self):
        print('\nEnrich dataset with formatted address data supplied by Google Maps:')
        helper, io, geo = self.Helper(), self.IO(), self.Geo()
        # Get raw property data
        data = helper.remove_duplicates(original_path=self.File.SUBSET,
                                        target_path=self.File.ADDRESSES,
                                        select=['url', 'address', 'city', 'country'],
                                        dedup=['url'])
        # Check if anything to work on
        if len(data) == 0:
            print('There are no new properties to work on.')
            return
        # Initiate Google Maps connection
        api_key = self.API().key(service='google', category='maps_key')
        gmaps = self.GoogleMaps(api_key)
        print("\nExtracting...")
        output = []
        io.pause(2)
        bar = self.tqdm(total=len(data), bar_format='{l_bar}{bar:50}{r_bar}{bar:-50b}')
        io.pause()
        for index in data.index:
            entry = data.loc[[index]]
            query = self._get_query(entry)
            raw_response = gmaps.geocode(query)
            parsed_response = self._geocoding_parser(raw_response) if raw_response else {}
            parsed_response['url'] = entry.url.item()
            parsed_response['query'] = query
            parsed_response['raw_response'] = raw_response
            output.append(parsed_response)
            bar.update(1)
        bar.close()
        io.pause()  # Prevents issues with the layout of update messages im terminal
        # Save
        output = self.pd.DataFrame(output).add_prefix('gmaps_')
        output = output.rename(columns={"gmaps_url": "url"}, errors="raise")
        helper.update_pq(data=output, path=self.File.ADDRESSES, dedup=['url'])
        print("\nCompleted.")

    def request_address(self, request):
        api_key = self.API().key(service='google', category='maps_key')
        gmaps = self.GoogleMaps(api_key)
        query = request.gmaps_address_request.item()
        raw_response = gmaps.geocode(query)
        parsed_response = self._geocoding_parser(raw_response) if raw_response else {}
        output = self.pd.DataFrame([parsed_response]).add_prefix('gmaps_') if parsed_response else None
        return output

    def _get_query(self, entry):
        address = entry.address.item().split(',')[0]
        city = entry.city.item()
        country = entry.country.item()
        query = f'{address}, {city}, {country}'
        return query

    def _geocoding_parser(self, response):
        elements = {}
        for i in response[0]['address_components']:
            type_name = i['types'][0]
            long_name = i['long_name']
            if type_name in ['street_number', 'route', 'political', 'postal_town', 'administrative_area_level_1',
                             'country', 'postal_code']:
                elements[type_name] = long_name
        elements['formatted_address'] = response[0]['formatted_address']
        elements['latitude'] = response[0]['geometry']['location']['lat']
        elements['longitude'] = response[0]['geometry']['location']['lng']
        elements['place_id'] = response[0]['place_id']
        return elements


if __name__ == '__main__':
    Address().address()
