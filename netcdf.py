import netCDF4
import numpy as np
import configparser


def load():
  config = configparser.ConfigParser()
  config.read('config.ini', encoding='utf-8')

  nc_s1 = netCDF4.Dataset(config['NetCDF_FILE']['nc_s1'], 'r')
  nc_s2 = netCDF4.Dataset(config['NetCDF_FILE']['nc_s2'], 'r')
  nc_s3 = netCDF4.Dataset(config['NetCDF_FILE']['nc_s3'], 'r')
  nc_s4 = netCDF4.Dataset(config['NetCDF_FILE']['nc_s4'], 'r')
  nc_s5 = netCDF4.Dataset(config['NetCDF_FILE']['nc_s5'], 'r')

  lon_s = nc_s1['longitude'][:]
  lat_s = nc_s1['latitude'][:]

  # 閏年に注意
  nc_s1 = nc_s1['MSL'][15:][:][:] / 100
  nc_s2 = nc_s2['MSL'][:][:][:] / 100
  nc_s3 = nc_s3['MSL'][:][:][:] / 100
  nc_s4 = nc_s4['MSL'][:][:][:] / 100
  nc_s5 = nc_s5['MSL'][:735][:][:] / 100
  nc_s = np.concatenate([nc_s1,nc_s2, nc_s3, nc_s4, nc_s5])

  return lon_s, lat_s, nc_s