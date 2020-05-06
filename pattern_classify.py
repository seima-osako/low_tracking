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
    df['dist'] = df.swifter.apply(dist, axis=1)
    df['angle'] = df.swifter.apply(angle, axis=1)
    self.df = df

  def Low_flg(self):
    start()
    sys.stderr.write("*** 低気圧本体による降雪フラグ付与 ***\n")
    self.df['type_P'] = self.df.swifter.apply(lambda x : 1 if (x['dist']<=400)&(x['P_flg']==1) else 0, axis=1)
    self.df['type_J'] = self.df.swifter.apply(lambda x : 1 if (x['dist']<=400)&(x['J_flg']==1) else 0, axis=1)

    t = self.df >> select(X.winter, X.datetime, X.type_P, X.type_J, X.Double_flg) >> \
        group_by(X.winter, X.datetime) >> mutate(P=np.max(X.type_P), J=np.max(X.type_J)) >> \
        select(X.winter, X.datetime, X.P, X.J, X.Double_flg) >> distinct()
    # 二つ玉と判定されている時のみ
    t['D'] = t.swifter.apply(lambda x : 1 if ((x['P']==1)&(x['J']==1)&(x['Double_flg']>=0)) else 0, axis=1)

    t = t.drop(columns='Double_flg')
    t = t.loc[t.groupby(['winter', 'datetime'])['D'].idxmax(),:]
    self.df = self.df >> inner_join(t, by=['winter', 'datetime']) >> select(~X.type_P ,~X.type_J)
    end()

  def Low_WM_flg(self):
    # 方位角0~135°
    start()
    sys.stderr.write("*** 引きの冬型による降雪フラグ付与(P, J, Dが1じゃないとき) ***\n")
    self.df['type_P'] = self.df.swifter.apply(lambda x : 1 if (x['P']!=1)&(x['J']!=1)&(x['D']!=1)&(x['dist']>400)&\
                          (x['angle']>=0)&(x['angle']<=135)&(x['WAMOI']>0)&(x['P_flg']==1) else 0, axis=1)
    self.df['type_J'] = self.df.swifter.apply(lambda x : 1 if (x['P']!=1)&(x['J']!=1)&(x['D']!=1)&(x['dist']>400)&\
                          (x['angle']>=0)&(x['angle']<=135)&(x['WAMOI']>0)&(x['J_flg']==1) else 0, axis=1)
    t = self.df >> select(X.winter, X.datetime, X.type_P, X.type_J, X.Double_flg) >> \
        group_by(X.winter, X.datetime) >> mutate(P_WM=np.max(X.type_P), J_WM=np.max(X.type_J)) >> \
        select(X.winter, X.datetime, X.P_WM, X.J_WM, X.Double_flg) >> distinct()
    t['D_WM'] = t.swifter.apply(lambda x : 1 if ((x['P_WM']==1)&(x['J_WM']==1)&(x['Double_flg']>=0)) else 0, axis=1)

    t = t.drop(columns='Double_flg')
    t = t.loc[t.groupby(['winter', 'datetime'])['D_WM'].idxmax(),:]
    self.df = self.df >> inner_join(t, by=['winter', 'datetime']) >> select(~X.type_P ,~X.type_J)
    end()

  def WM_flg(self):
    start()
    sys.stderr.write("*** 冬型による降雪フラグ付与 ***\n")
    self.df['WM'] = self.df.swifter.apply(lambda x : 1 if (x['P']!=1)&(x['J']!=1)&(x['D']!=1)&(x['P_WM']!=1)&(x['J_WM']!=1)&(x['D_WM']!=1)&\
                            (x['WAMOI']>0)&(x['P_flg']!=1)&(x['J_flg']!=1) else 0, axis=1)
    end()

  def Other(self):
    start()
    sys.stderr.write("*** どれにも分類されたかった降雪フラグ付与 ***\n")
    self.df['Other'] = self.df.swifter.apply(lambda x : 1 if (x['P']!=1)&(x['J']!=1)&(x['D']!=1)&(x['P_WM']!=1)&(x['J_WM']!=1)&(x['D_WM']!=1)&(x['WM']!=1) else 0, axis=1)
    end()
    
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



