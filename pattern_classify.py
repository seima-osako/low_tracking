import export as et
import timeutil as tu

import sys
import pyproj
import sqlite3
import numpy as np
import pandas as pd
from dfply import *
import configparser

def dist(x):
    grs80 = pyproj.Geod(ellps='GRS80')
    angle, inverse_angle, dist = grs80.inv(x['lon_s'], x['lat_s'], x['lon'], x['lat'])
    return dist / 1000

def angle(x):
    grs80 = pyproj.Geod(ellps='GRS80')
    angle, inverse_angle, dist = grs80.inv(x['lon_s'], x['lat_s'], x['lon'], x['lat'])
    return angle

class Pattern:
  def __init__(self, df):
    df['dist'] = df.apply(dist, axis=1)
    df['angle'] = df.apply(angle, axis=1)
    self.df = df >> distinct()

  def Low_flg(self):
    tu.start()
    sys.stderr.write("*** 低気圧本体による降雪フラグ付与 ***\n")
    self.df['type0']=self.df.apply(lambda x : 1 if (x['dist']<=400)&((x['P_flg']==1)|(x['J_flg']==1)) else 0, axis=1)
    # 同じ日時でも異なる低気圧ログがあるため、共通の日時は全てに再度フラグを振り直す
    Low = self.df >> filter_by(X.type0==1) >> select(X.datetime, X.station, X.type0) >>rename(Low= X.type0)
    self.df = self.df >> left_join(Low, by=['datetime', 'station']) >> select(~X.type0) >> distinct()
    tu.end()

  def Low_WM_flg(self):
    tu.start()
    sys.stderr.write("*** 引きの冬型による降雪フラグ付与(Lowが1じゃないとき) ***\n")
    self.df['type1'] = self.df.apply(lambda x : 1 if (x['Low']!=1)&(x['dist']>400)&\
                          (x['angle']>=-45)&(x['angle']<=135)&(x['WAMOI']>0)&((x['P_flg']==1)|(x['J_flg']==1)) else 0, axis=1)
    Low_WM = self.df >> filter_by(X.type1==1) >> select(X.datetime, X.station, X.type1) >>rename(Low_WM= X.type1)
    self.df = self.df >> left_join(Low_WM, by=['datetime', 'station']) >> select(~X.type1) >> distinct()
    tu.end()

  def WM_flg(self):
    tu.start()
    sys.stderr.write("*** 冬型による降雪フラグ付与 ***\n")
    self.df['type2'] = self.df.apply(lambda x : 1 if (x['Low']!=1)&(x['Low_WM']!=1)&(x['WAMOI']>0)&(x['P_flg']!=1)&(x['J_flg']!=1) else 0, axis=1)
    WM = self.df >> filter_by(X.type2==1) >> select(X.datetime, X.station, X.type2) >>rename(WM= X.type2)
    self.df = self.df >> left_join(WM, by=['datetime', 'station']) >> select(~X.type2) >> distinct()
    tu.end()

  def Other_flg(self):
    tu.start()
    sys.stderr.write("*** どれにも分類されたかった降雪フラグ付与 ***\n")
    self.df['type3'] = self.df.apply(lambda x : 1 if (x['Low']!=1)&(x['Low_WM']!=1)&(x['WM']!=1) else 0, axis=1)
    Other = self.df >> filter_by(X.type3==1) >> select(X.datetime, X.station, X.type3) >>rename(Other= X.type3)
    self.df = self.df >> left_join(Other, by=['datetime', 'station']) >> select(~X.type3) >> distinct()
    tu.end()
    return self.df

if __name__ == '__main__':
  config = configparser.ConfigParser()
  config.read('config.ini', encoding='utf-8')
  # 温帯低気圧トラッキングテーブルを読み込む
  DB_PATH_C = config['Classify']['DB_PATH']
  DB_NAME_C = config['Classify']['DB_NAME']
  file_sqlite_c = f'{DB_PATH_C}{DB_NAME_C}.db'
  conn = sqlite3.connect(file_sqlite_c)
  df = pd.read_sql_query(f'SELECT * FROM {DB_NAME_C}', conn)
  
  #冬型指数のテーブルを読み込む
  MOI = pd.read_csv(config['MOI']['OUT_DATA_PATH'])
  # 降雪深データを読み込む
  data = pd.read_csv(config['Snowfall']['PATH'])
  data = data >> inner_join(MOI, by='datetime') >> group_by(X.station, X.yyyymmdd) >> mutate(d_snow=np.sum(X.h_snow)) >> \
    select(X.datetime, X.yyyymmdd, X.hh, X.station, X.d_snow, X.h_snow, X.lat_s, X.lon_s, X.WAMOI)

  # 外部結合させる順番に注意！！
  data = data >> left_join(df, by='datetime')

  main = Pattern(data)
  main.Low_flg()
  main.Low_WM_flg()
  main.WM_flg()
  output = main.Other_flg()

  DB_PATH = config['Snowfall']['DB_PATH']
  DB_NAME = config['Snowfall']['DB_NAME']
  et.data_to_db(output, DB_PATH, DB_NAME)



