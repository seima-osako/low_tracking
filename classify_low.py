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

    sys.stderr.write("*** ERAの格子点をH3メッシュ変換し、文字列　→　番号に変更する ***\n")
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

    return self.df




if __name__ == '__main__':
  config = configparser.ConfigParser()
  config.read('config.ini', encoding='utf-8')
  nc_s1 = netCDF4.Dataset(config['NetCDF_FILE']['nc_s1'], 'r')

  DB_PATH = config['Tracking']['DB_PATH']
  DB_NAME = config['Tracking']['DB_NAME']
  file_sqlite = f'{DB_PATH}{DB_NAME}.db'

  main = Classify(file_sqlite, DB_NAME)
  df = main.geo_to_mesh(nc_s1)
  print(df)