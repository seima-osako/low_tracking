from h3 import h3


def GeoToH3(lon, lat, resolution):
  return h3.geo_to_h3(lat, lon, resolution)

def H3ToGeo(h3_id):
  return h3.h3_to_geo(h3_id)

def H3ToGeoBoundary(h3_id):
  return h3.h3_to_geo_boundary(h3_id)