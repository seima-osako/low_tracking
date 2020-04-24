import sqlite3

def data_to_db(data, db_path, db_name):
  file_sqlite = f'{db_path}{db_name}.db'
  conn = sqlite3.connect(file_sqlite)
  data.to_sql(db_name, conn, if_exists='replace', index=None)
  conn.close()