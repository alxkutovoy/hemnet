class Parser(object):

    import numpy as np
    import pandas as pd
    import re

    from bs4 import BeautifulSoup

    # Operational methods

    def _soup(self, content):
        return self.BeautifulSoup(content, "html.parser")

    def _raw_address(self, content):
        return self._soup(content).find("h1", {"class": "sold-property__address"})

    def _raw_property(self, content):
        return self._soup(content).find("p", {"class": "sold-property__metadata"})

    def _raw_map(self, content):
        return self._soup(content).find("div", {"class": "property-map"})

    def _raw_endprice(self, content):
        return self._soup(content).find("span", {"class": "sold-property__price-value"})

    def _raw_propdet(self, content):
        return self._soup(content).find("div", {"class": "sold-property__details"})

    def _raw_broker(self, content):
        return self._soup(content).find("div", {"class": "broker-contact-card__information"})

    # Cleaners

    def _clean_propdet(self, raw):
        return ' '.join(raw.text.replace(u'\xa0', u'').replace('\n', ' ').split())

    # Data extraction

    def get_address(self, content):
        raw = self._raw_address(content)
        try:
            return self.re.search('\n  (.*)\n', raw.get_text()).group(1)
        except Exception as e:
            return None

    def get_proptype(self, content):
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

    def get_owntype(self, content):
        raw = self._raw_property(content)
        try:
            owntype_parsed = self.re.search('\n  (.*) -', raw.get_text())
            return owntype_parsed.group(1)
        except Exception as e:
            return None

    def get_neighborhood(self, content):
        raw = self._raw_property(content)
        try:
            neighborhood_parsed = self.re.search('-\n\n    (.*),', raw.get_text())
            return neighborhood_parsed.group(1)
        except Exception as e:
            return None

    def get_municipality(self, content):
        raw = self._raw_property(content)
        try:
            municipality_parsed = self.re.search(',\n  (.*)\n\n', raw.get_text())
            return municipality_parsed.group(1)
        except Exception as e:
            return None

    def get_coordinates(self, content):
        raw = self._raw_map(content)
        try:
            coordinates_parsed = self.re.search('\[(.*)\]', str(raw))
            return self.np.float_(coordinates_parsed.group(1).split(',')).tolist()
        except Exception as e:
            return None

    def get_endprice(self, content):
        raw = self._raw_endprice(content)
        try:
            return int(self.re.sub("[^0-9]", "", raw.get_text()))
        except Exception as e:
            return None

    def get_pricesqm(self, content):
        raw = self._raw_propdet(content)
        try:
            pricesqm_parsed = self.re.search('(?<=Pris per kvadratmeter )([^ |\-]+)', self._clean_propdet(raw))
            return int(pricesqm_parsed.group(1))
        except Exception as e:
            return None

    def get_startprice(self, content):
        raw = self._raw_propdet(content)
        try:
            startprice_parsed = self.re.search('(?<=Begärt pris )([^ |\-]+)', self._clean_propdet(raw))
            return int(startprice_parsed.group(1))
        except Exception as e:
            return None

    def get_rum(self, content):
        raw = self._raw_propdet(content)
        try:
            rum_parsed = self.re.search('(?<=Antal rum )([^ |\-]+)', self._clean_propdet(raw))
            return float(rum_parsed.group(1).replace(',', '.'))
        except Exception as e:
            return None

    def get_area(self, content):
        raw = self._raw_propdet(content)
        try:
            area_parsed = self.re.search('(?<=Boarea )([^ |\-]+)', self._clean_propdet(raw))
            return float(area_parsed.group(1).replace(',', '.'))
        except Exception as e:
            return None

    def get_utils(self, content):
        raw = self._raw_propdet(content)
        try:
            utils_parsed = self.re.search('(?<=Avgift/månad )([^ |\-]+)', self._clean_propdet(raw))
            return int(utils_parsed.group(1))
        except Exception as e:
            return None

    def get_buildyear(self, content):
        raw = self._raw_propdet(content)
        try:
            buildyear_parsed = self.re.search('(?<=Byggår )([^_|\-]+)', self._clean_propdet(raw))
            parsed = ''.join(filter(str.isdigit, buildyear_parsed.group(1)))
            return parsed if parsed else 'N/A'
        except Exception as e:
            return None

    def get_brokerfullname(self, content):
        raw = self._raw_broker(content)
        try:
            brokerfullname_parsed = ' '.join(raw.strong.get_text().replace('\n', '').strip().split()).split(' ')
            return brokerfullname_parsed
        except Exception as e:
            return None

    def get_brokeragency(self, content):
        raw = self._raw_broker(content)
        try:
            brokeragency_parsed = raw.find("a", {"class": "broker-link"})
            return brokeragency_parsed.get_text().replace('\n', '').strip()
        except Exception as e:
            return None

    def get_brokerphone(self, content):
        raw = self._raw_broker(content)
        try:
            brokerphone_parsed = raw.find("a", {"class": "broker-contact__link"})
            return brokerphone_parsed.get('href').replace('tel:', '')
        except Exception as e:
            return None
