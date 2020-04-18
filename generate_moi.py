import netcdf as nc
import timeutil as tu

import sys
import configparser
from datetime import timedelta
from datetime import datetime as dt


lon_s, lat_s, nc_s = nc.load()

config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')
END_TIME = int(config['Detection']['END_TIME'])

tu.start()

hpa_list = []
for t in range(0, END_TIME):
  hpa = ((nc_s[t][60][80] -  nc_s[t][60][160]) + (nc_s[t][100][80] -  nc_s[t][100][160])) / 2 
  hpa_list.append(hpa)

tu.end()