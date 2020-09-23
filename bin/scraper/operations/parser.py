class Parser(object):

    import json
    import numpy as np
    import pandas as pd
    import re

    from bs4 import BeautifulSoup

    # Operational methods

    def _soup(self, content):
        return self.BeautifulSoup(content, "html.parser")

    def _raw_meta(self, content):
        try:
            data_layer = self.re.search('dataLayer = (.*);', content).group(1)
            return self.json.loads(data_layer)
        except Exception as e:
            return None

    def _raw_property(self, content):
        return self._soup(content).find("p", {"class": "sold-property__metadata"})

    def _raw_propdet(self, content):
        return self._soup(content).find("div", {"class": "sold-property__details"})

    # Cleaners

    def _clean_propdet(self, raw):
        return ' '.join(raw.text.replace(u'\xa0', u'').replace('\n', ' ').split())

    # Data extraction

    def get_property_id(self, content):
        raw = self._raw_meta(content)
        try:
            return int(raw[1]["property"]["id"])
        except Exception as e:
            return None

    def get_sold_at_date(self, content):
        raw = self._raw_meta(content)
        try:
            return self.pd.to_datetime(raw[2]["sold_property"]["sold_at_date"])
        except Exception as e:
            return None

    def get_sold_property_id(self, content):
        raw = self._raw_meta(content)
        try:
            return int(raw[2]["sold_property"]["id"])
        except Exception as e:
            return None

    def get_property_type(self, content):
        raw = self._raw_property(content)
        try:
            return raw.title.get_text()
        except Exception as e:
            return None

    def get_ts(self, content):
        raw = self._raw_property(content)
        try:
            return self.pd.to_datetime(raw.time.get('datetime'))
        except Exception as e:
            return None

    def get_ownership_type(self, content):
        raw = self._raw_property(content)
        try:
            ownership_type_parsed = self.re.search('\n  (.*) -', raw.get_text())
            return ownership_type_parsed.group(1)
        except Exception as e:
            return None

    def get_country(self, content):
        raw = self._raw_meta(content)
        try:
            return raw[2]["sold_property"]["locations"]["country"]
        except Exception as e:
            return None

    def get_region(self, content):
        raw = self._raw_meta(content)
        try:
            return raw[2]["sold_property"]["locations"]["region"]
        except Exception as e:
            return None

    def get_urban_area(self, content):
        raw = self._raw_meta(content)
        try:
            return raw[2]["sold_property"]["locations"]["city"]
        except Exception as e:
            return None

    def get_municipality(self, content):
        raw = self._raw_meta(content)
        try:
            return raw[2]["sold_property"]["locations"]["municipality"]
        except Exception as e:
            return None

    def get_location(self, content):
        raw = self._raw_meta(content)
        try:
            output = raw[2]["sold_property"]["location"]
            return output if isinstance(output, str) else 'Error'
        except Exception as e:
            return None

    def get_city(self, content):
        raw = self._raw_meta(content)
        try:
            return raw[2]["sold_property"]["locations"]["postal_city"]
        except Exception as e:
            return None

    def get_district(self, content):
        raw = self._raw_meta(content)
        try:
            return raw[2]["sold_property"]["locations"]["district"]
        except Exception as e:
            return None

    def get_neighborhood(self, content):
        raw = self._raw_property(content)
        try:
            neighborhood_parsed = self.re.search('-\n\n    (.*),', raw.get_text())
            return neighborhood_parsed.group(1)
        except Exception as e:
            return None

    def get_address(self, content):
        raw = self._raw_meta(content)
        try:
            return raw[2]["sold_property"]["street_address"]
        except Exception as e:
            return None

    def get_street(self, content):
        raw = self._raw_meta(content)
        try:
            return raw[2]["sold_property"]["locations"]["street"]
        except Exception as e:
            return None

    def get_coordinates(self, content):  # TODO: Extract as a dict
        raw = self._soup(content).find("div", {"class": "property-map"})
        try:
            coordinates_parsed = self.re.search('\[(.*)\]', str(raw))
            return self.np.float_(coordinates_parsed.group(1).split(',')).tolist()
        except Exception as e:
            return None

    def get_start_price(self, content):
        raw = self._raw_meta(content)
        try:
            return int(raw[2]["sold_property"]["price"])
        except Exception as e:
            return None

    def get_end_price(self, content):
        raw = self._raw_meta(content)
        try:
            return int(raw[2]["sold_property"]["selling_price"])
        except Exception as e:
            return None

    def get_price_sqm(self, content):
        raw = self._raw_propdet(content)
        try:
            pricesqm_parsed = self.re.search('(?<=Pris per kvadratmeter )([^ |\-]+)', self._clean_propdet(raw))
            return int(pricesqm_parsed.group(1))
        except Exception as e:
            return None

    def get_rums(self, content):
        raw = self._raw_meta(content)
        try:
            return float(raw[2]["sold_property"]["rooms"])
        except Exception as e:
            return None

    def get_area(self, content):
        raw = self._raw_meta(content)
        try:
            return float(raw[2]["sold_property"]["living_area"])
        except Exception as e:
            return None

    def get_utils(self, content):
        raw = self._raw_propdet(content)
        try:
            utils_parsed = self.re.search('(?<=Avgift/månad )([^ |\-]+)', self._clean_propdet(raw))
            return int(utils_parsed.group(1))
        except Exception as e:
            return None

    def get_build_year(self, content):
        raw = self._raw_propdet(content)
        try:
            buildyear_parsed = self.re.search('(?<=Byggår )([^_|\-]+)', self._clean_propdet(raw))
            parsed = ''.join(filter(str.isdigit, buildyear_parsed.group(1)))
            return parsed
        except Exception as e:
            return None

    def get_broker_full_name(self, content):
        raw = self._soup(content)
        try:
            broker_data = raw.find("div", {"class": "broker-contact-card__information"})
            return ' '.join(broker_data.strong.get_text().replace('\n', '').strip().split()).split(' ')
        except Exception as e:
            return None

    def get_broker_agency_id(self, content):
        raw = self._raw_meta(content)
        try:
            return raw[2]["sold_property"]["broker_agency_id"]
        except Exception as e:
            return None

    def get_broker_agency(self, content):
        raw = self._raw_meta(content)
        try:
            return raw[2]["sold_property"]["broker_agency"]
        except Exception as e:
            return None

    def get_broker_phone(self, content):
        raw = self._soup(content)
        try:
            phone_parsed = raw.find('a', {"class": "broker-contact__button-link gtm-tracking-broker-phone"})
            return self.re.search('tel:(.*)', phone_parsed.get("href")).group(1)
        except Exception as e:
            return None

    def get_broker_email(self, content):
        raw = self._soup(content)
        try:
            email_parsed = raw.find('a', {"class": "broker-contact__button-link gtm-tracking-broker-email"})
            return self.re.search('mailto:(.*)\?', email_parsed.get("href")).group(1)
        except Exception as e:
            return None
