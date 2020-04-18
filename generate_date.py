import configparser
import pandas as pd
from datetime import timedelta
from datetime import datetime


def date_list():
  config = configparser.ConfigParser()
  config.read('config.ini', encoding='utf-8')
  
  START_DATE = config['Setting_date']['START_DATE']
  END_DATE = config['Setting_date']['END_DATE']

  strdt = datetime.strptime(START_DATE, '%Y-%m-%d')  # 開始日
  enddt = datetime.strptime(END_DATE, '%Y-%m-%d')  # 終了日
  hours_num = ((enddt - strdt).days + 1) * 24
  # timedeltaのオプションをhoursに変更
  hourlist = map(lambda x, y = strdt: y + timedelta(hours=x), range(hours_num))
  
  date = []
  for d in hourlist:
    date.append(d.strftime("%Y-%m-%d %H"))
  dt = pd.DataFrame(date, columns=['datetime']).reset_index()
  dt = dt.rename(columns={'index':'datetime', 'datetime':'yyyymmddhh'})
  
  return dt