class Geo(object):

    from math import sin, cos, sqrt, atan2, radians

    def gcs_to_dist(self, point_a, point_b):
        lat_a, lon_a = self.radians(float(point_a[0])), self.radians(float(point_a[1]))
        lat_b, lon_b = self.radians(float(point_b[0])), self.radians(float(point_b[1]))
        r = 6378.137  # Radius of Earth in km
        dlon, dlat = lon_b - lon_a, lat_b - lat_a
        a = self.sin(dlat / 2) ** 2 + self.cos(lat_a) * self.cos(lat_b) * self.sin(dlon / 2) ** 2
        c = 2 * self.atan2(self.sqrt(a), self.sqrt(1 - a))
        distance = r * c * 1000  # Distance in meters
        return distance
