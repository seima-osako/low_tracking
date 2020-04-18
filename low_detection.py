import netcdf as nc
import timeutil as tu

import os
import sys
import cv2
import pyproj
import numpy as np
import pandas as pd
import configparser
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor


def Low_detection(msl, lon_s, lat_s):
  X, Y = np.meshgrid(lon_s, lat_s)
  row = np.zeros((0,5))
  for i in range(20,160): # 20~56
      for j in range(40,200): # 120~160
          if (msl[i,j] < msl[i-1,j-1]) & (msl[i,j] < msl[i-1,j]) & (msl[i,j] < msl[i-1,j+1]) & (msl[i,j] < msl[i,j-1]) & \
            (msl[i,j] < msl[i,j+1]) & (msl[i,j] < msl[i+1,j-1]) & (msl[i,j] < msl[i+1,j]) & (msl[i,j] < msl[i+1,j+1]):
              
              row = np.vstack((row, np.hstack((i,j,X[i,j],Y[i,j],msl[i,j]))))
  
  rows = pd.DataFrame(row, columns=['lon_id','lat_id','lon','lat','hPa'])
  
  # 周囲3°(0.25*12グリッド＝約300km）内に複数低気圧が検出されていた場合はSLPが一番小さいグリッドを採用
  d = pd.DataFrame()
  for r in range(0,len(rows)):
      try:
          lon = rows.iat[r,2]
          lat = rows.iat[r,3]
          d1 = rows.query(f"lon <= {lon}+3 & lon >= {lon}-3 & lat <= {lat}+3 & lat >= {lat}-3")
          d2 = d1[d1['hPa']==d1['hPa'].min()]
          d = pd.concat([d,d2])
      except:
          pass
      d = d.drop_duplicates() # ダブりを削除
  
  return d.reset_index(drop=True)


config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')

OUT_DIR = config['Detection']['OUT_DIR']
if os.path.isdir(OUT_DIR):
  pass
else:
  os.makedirs(OUT_DIR)

tu.start()

sys.stderr.write("*** NetCDFファイル読み込み中 ***\n")
lon_s, lat_s, nc_s = nc.load()
sys.stderr.write("*** 読み込み完了 ***\n")

tu.end()

END_TIME = int(config['Detection']['END_TIME'])

with ProcessPoolExecutor(max_workers=8) as ppex:
  for t in tqdm(range(0, END_TIME)):
    msl = cv2.GaussianBlur(nc_s[t][:][:], ksize=(3, 3), sigmaX=2) # ガウシアンフィルタ
    tmp = ppex.submit(Low_detection, msl, lon_s, lat_s).result()
    tmp = pd.DataFrame(tmp, columns=['lon_id','lat_id','lon','lat','hPa'])
    tmp.to_csv(f'{OUT_DIR}{t}.csv',index=None)