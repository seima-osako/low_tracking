import export as et
import h3_mesh as hm
import timeutil as tu

import sys
import sqlite3
import netCDF4
from dfply import *
import configparser


class Classify:
  def __init__(self, file_sqlite, table_name):
    conn = sqlite3.connect(file_sqlite)
    df = pd.read_sql_query(f'SELECT * FROM {table_name}', conn)
    self.df = df >> select(X.low_id, X.datetime, X.lon, X.lat, X.hPa)


  def geo_to_mesh(self, nc_s):
    tu.start()
    sys.stderr.write("*** 低気圧の中心位置情報をH3メッシュ変換 ***\n")
    self.df['h3_1_id'] = self.df.apply(lambda x : hm.GeoToH3(x['lon'], x['lat'], 1), axis=1)
    self.df['h3_2_id'] = self.df.apply(lambda x : hm.GeoToH3(x['lon'], x['lat'], 2), axis=1)
    tu.end()

    tu.start()
    sys.stderr.write("*** ERAの格子点をH3メッシュ変換し、文字列→番号に変更する ***\n")
    lat_s = nc_s['latitude'][:]
    lon_s = nc_s['longitude'][:]
    x, y = np.meshgrid(lon_s,lat_s)
    xy = np.concatenate([x.reshape(-1, 1),  y.reshape(-1, 1)], axis=1)
    xy = pd.DataFrame(xy, columns=['lon', 'lat'])
    xy['h3_1_id'] = xy.apply(lambda x : hm.GeoToH3(x['lon'], x['lat'], 1), axis=1)
    xy['h3_2_id'] = xy.apply(lambda x : hm.GeoToH3(x['lon'], x['lat'], 2), axis=1)
    
    def h3_mesh_join(df, xy, n, h3_id):
      h3_ids = xy[h3_id].drop_duplicates().reset_index(drop=True)
      ids = []
      h3ids = []
      for id, i in enumerate(h3_ids):
        ids.append(id)
        h3ids.append(i)
    
      a = pd.DataFrame(ids).rename(columns={0:f'id{n}'})
      b = pd.DataFrame(h3ids).rename(columns={0: h3_id })
      c = pd.concat([a,b], axis=1)
      df = df >> inner_join(c, by=h3_id)
      return df

    df = h3_mesh_join(self.df, xy, 1, 'h3_1_id')
    self.df = h3_mesh_join(df, xy, 2, 'h3_2_id')
    tu.end()

  def extraction_low(self):
    tu.start()
    sys.stderr.write("*** 24時間以上持続した低気圧を抽出し、低気圧ごとにランクを付与する ***\n")
    self.df1 = self.df >> group_by(X.low_id) >> arrange(X.datetime) >> mutate(rk=row_number(X.datetime)) >> \
        mutate(diff_time=np.max(X.datetime)-np.min(X.datetime)) >> \
        filter_by(X.diff_time>23) >> select(~X.diff_time) >> group_by(X.low_id) >> mutate(max_rk=np.max(X.rk))
    tu.end()

  def make_P_flg(self):
    tu.start()
    sys.stderr.write("*** 南岸低気圧判定 ***\n")
    # 指定領域で発生した低気圧にフラグを付与
    self.df1['P_ini_flg'] = self.df1.apply(lambda x: 1 if (x['rk']==1)&(x['id1'] in [29, 30, 31, 37, 38, 39, 45, 46]) else 0, axis=1)
    # id1の2メッシュ以上のログを残した場合にフラグを付与
    P_middle_low = self.df1 >> select(X.low_id, X.id1) >> distinct() >> group_by(X.low_id) >> mutate(freq=np.size(X.id1)) >> \
      mutate(P_middle_flg=if_else(X.freq>1, 1, 0)) >> filter_by(X.P_middle_flg==1) >> select(X.low_id) >>distinct()
    # 消滅時に指定領域外で検知された場合にフラグを付与
    self.df1['P_last_flg'] = self.df1.apply(lambda x: 1 if (x['rk']==x['max_rk'])&(x['lat']>32) else 0, axis=1)
    # P_ini_flg=1の低気圧
    P_ini_low = self.df1 >> filter_by(X.P_ini_flg==1) >> select(X.low_id) >> distinct()
    # P_last_flg=1の低気圧
    P_last_low = self.df1 >> filter_by(X.P_last_flg==1) >> select(X.low_id) >> distinct()
    # 南岸低気圧と判定されたlow_idを残す
    P_flg = P_ini_low >> inner_join(P_middle_low, by='low_id') >> inner_join(P_last_low, by='low_id') >> mutate(P_flg=1)
    # 南岸低気圧フラグ完成
    self.df2 = self.df1 >> left_join(P_flg, by='low_id') >> select(~X.P_ini_flg, ~X.P_last_flg)
    tu.end()

  def make_J_flg(self):
    tu.start()
    sys.stderr.write("*** 日本海低気圧判定 ***\n")
    # 指定領域で発生した低気圧にフラグを付与
    self.df2['J_ini_flg'] = self.df2.apply(lambda x: 1 if (x['rk']==1)&(x['id1'] in [14, 17, 21, 23, 25, 27]) else 0, axis=1)
    # 指定領域で検知された場合にフラグを付与
    self.df2['J_middle_flg'] = self.df2.apply(lambda x: 1 if (x['P_flg']!=1)&(x['id2'] in [81, 97, 98, 115, 116, 130, 134]) else 0, axis=1)
    J_ini_low = self.df2 >> filter_by(X.J_ini_flg==1) >> select(X.low_id)
    J_middle_low = self.df2 >> filter_by(X.J_middle_flg==1) >> select(X.low_id) >> distinct()
    # id1の2メッシュ以上のログを残した場合にフラグを付与
    J_last_low = self.df2 >> select(X.low_id, X.id1) >> distinct() >> group_by(X.low_id) >> mutate(freq=np.size(X.id1)) >> \
      mutate(J_last_flg=if_else(X.freq>1, 1, 0)) >> filter_by(X.J_last_flg==1) >> select(X.low_id) >>distinct()
    # 日本海低気圧と判定されたlow_idを残す
    J_flg = J_ini_low >> inner_join(J_middle_low, by='low_id') >> inner_join(J_last_low, by='low_id') >> mutate(J_flg=1)
    self.df3 = self.df2 >> left_join(J_flg, by='low_id') >> select(~X.J_ini_flg, ~X.J_middle_flg)
    tu.end()

  def make_Double_flg(self):
    sys.stderr.write("*** 二つ玉低気圧判定 ***\n")
    P_low = self.df3 >> filter_by(X.P_flg==1) >> select(X.datetime, X.low_id, X.lon, X.lat, X.P_flg) >> \
      rename(P_low_id=X.low_id, P_lon=X.lon, P_lat=X.lat)

    J_low = self.df3 >> filter_by(X.J_flg==1) >> select(X.datetime, X.low_id,  X.lon, X.lat, X.J_flg) >> \
      rename(J_low_id=X.low_id, J_lon=X.lon, J_lat=X.lat)

    P_low >> inner_join(J_low, by='datetime')
    # 南岸低気圧と日本海低気圧が共存（日時が一致）した低気圧情報を残す
    join = P_low >> inner_join(J_low, by='datetime')
    # 両低気圧が指定領域で位置情報を残した場合にフラグを付与
    join['Double_flg']=join.apply(lambda x : 1 if (x['P_lon']>=132)&(x['P_lon']<=150)&(x['P_lat']>=34)&(x['P_lat']<=46)& \
      (x['J_lon']>=132)&(x['J_lon']<=150)&(x['J_lat']>=34)&(x['J_lat']<=46) else 0 , axis=1)
    # 両低気圧が共存した時、その状態が5時間以上持続した場合を二つ玉低気圧とする
    Double_flg = join >> filter_by(X.Double_flg==1) >> group_by(X.P_low_id, X.J_low_id) >> mutate(freq=np.size(X.P_low_id))>> \
      filter_by(X.freq>4) >> select(X.P_low_id, X.J_low_id, X.Double_flg) >> distinct()
    # 二つ玉低気圧を識別するためのカラムを追加
    Double_flg=Double_flg.reset_index().reset_index().drop(['index', 'Double_flg'], axis=1) >> rename(Double_flg=X.level_0)
    # 縦にunionさせつつ、low_idにカラムを変更する
    P = Double_flg >> select(X.P_low_id, X.Double_flg) >> rename(low_id=X.P_low_id)
    J = Double_flg >> select(X.J_low_id, X.Double_flg) >> rename(low_id=X.J_low_id)
    Double_flg = P >> union(J)
    # 二つ玉低気圧フラグ完成
    d = self.df3 >> left_join(Double_flg, by='low_id')

    d = d >> select(~X.h3_1_id, ~X.h3_2_id, ~X.id1, ~X.id2,  ~X.rk, ~X.max_rk)
    data = d.drop_duplicates()
    return data



if __name__ == '__main__':
  config = configparser.ConfigParser()
  config.read('config.ini', encoding='utf-8')
  nc_s1 = netCDF4.Dataset(config['NetCDF_FILE']['nc_s1'], 'r')

  DB_PATH_T = config['Tracking']['DB_PATH']
  DB_NAME_T = config['Tracking']['DB_NAME']
  file_sqlite_t = f'{DB_PATH_T}{DB_NAME_T}.db'

  main = Classify(file_sqlite_t, DB_NAME_T)
  main.geo_to_mesh(nc_s1)
  main.extraction_low()
  main.make_P_flg()
  main.make_J_flg()
  # 温帯低気圧分類テーブル完成
  data = main.make_Double_flg()

  #冬型指数のテーブルを読み込む
  MOI = pd.read_csv(config['MOI']['OUT_DATA_PATH'])
  # 外部結合させる順番に注意！！
  data = MOI >> left_join(data, by='datetime') >> \
    select(X.datetime, X.yyyymmddhh, X.low_id, X.lon, X.lat, X.hPa, X.WAMOI, X.P_flg, X.J_flg, X.Double_flg)

  DB_PATH_C = config['Classify']['DB_PATH']
  DB_NAME_C = config['Classify']['DB_NAME']
  et.data_to_db(data, DB_PATH_C, DB_NAME_C)


