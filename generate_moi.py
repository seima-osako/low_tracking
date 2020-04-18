import netcdf as nc
import timeutil as tu
import generate_date as gd

import sys
import configparser
import pandas as pd


class MOI:
  def __init__(self):    
    tu.start()
    sys.stderr.write("*** NetCDFファイル読み込み中 ***\n")
    self.lon_s, self.lat_s, self.nc_s = nc.load()
    sys.stderr.write("*** 読み込み完了 ***\n")
    tu.end()

  def moi_list(self, END_TIME):

    hpa_list = []
    for t in range(0, END_TIME):
      hpa = ((self.nc_s[t][60][80] - self.nc_s[t][60][160]) + (self.nc_s[t][100][80] - self.nc_s[t][100][160])) / 2
      hpa_list.append(hpa)
    
    MOI = pd.DataFrame(hpa_list).reset_index().rename(columns={0:'WAMOI', 'index':'datetime'})
    
    return MOI


if __name__ == '__main__':
  config = configparser.ConfigParser()
  config.read('config.ini', encoding='utf-8')
  path = config['MOI']['OUT_DATA_PATH']
  END_TIME = int(config['Detection']['END_TIME'])
  
  m = MOI()
  moi = m.moi_list(END_TIME)
  yyyymmddhh = gd.date_list()
  MOI = pd.merge(moi, yyyymmddhh, on='datetime')
  MOI['datetime'] = MOI['datetime'].astype(int)

  MOI.to_csv(path, index=None)