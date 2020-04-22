import h3_mesh as hm
import timeutil as tu


import sys
import sqlite3
from dfply import *
import configparser


class Classify:
  def __init__(self, file_sqlite, table_name):
    conn = sqlite3.connect(file_sqlite)
    df = pd.read_sql_query(f'SELECT * FROM {table_name}', conn)
    self.df = df >> select(X.low_id, X.date, X.datetime, X.lon, X.lat, X.hPa)


  def geo_to_mesh(self):
    tu.start()
    sys.stderr.write("*** H3メッシュ変換 ***\n")
    self.df['h3_1_id'] = self.df.apply(lambda x : hm.GeoToH3(x['lon'], x['lat'], 1), axis=1)
    self.df['h3_2_id'] = self.df.apply(lambda x : hm.GeoToH3(x['lon'], x['lat'], 2), axis=1)
    tu.end()

    return self.df

if __name__ == '__main__':
  config = configparser.ConfigParser()
  config.read('config.ini', encoding='utf-8')

  DB_PATH = config['Tracking']['DB_PATH']
  DB_NAME = config['Tracking']['DB_NAME']
  file_sqlite = f'{DB_PATH}{DB_NAME}.db'

  main = Classify(file_sqlite, DB_NAME)
  df = main.geo_to_mesh()