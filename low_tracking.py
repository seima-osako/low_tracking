import export as et

import sys
import pandas as pd
import configparser
from tqdm import tqdm

config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')

END_TIME = int(config['Detection']['END_TIME'])
OUT_DIR = config['Detection']['OUT_DIR']

DB_PATH = config['Tracking']['DB_PATH']
DB_NAME = config['Tracking']['DB_NAME']


sys.stderr.write("*** 追跡中 ***\n")
output = pd.DataFrame(columns=['lon_id','lat_id','lon','lat','hPa','datetime','low_id'])


for t in tqdm(range(0, END_TIME)):
  first_low = pd.read_csv(f'{OUT_DIR}{t}.csv')
  low_id = len(output) - 1 # 低気圧idを連番にするため
  for i in range(0,len(first_low)):
    number = 0
    d1 = first_low.iloc[i : i+1]
    d1['datetime'] = t
    ini_lon = d1.iat[0,2]
    ini_lat = d1.iat[0,3]
    for j in range(t+1,100): # 100時間も追跡しないでしょ？
      try:
        next_low = pd.read_csv(f'{OUT_DIR}{j}.csv')
        if number == 0: # 低気圧が初めて検知された場合（開始番号が0）は発生開始位置情報を参照
          d2 = next_low[(((next_low['lon']>=ini_lon)&(next_low['lon']<=ini_lon+2))&((next_low['lat']<=ini_lat+1.5)&(next_low['lat']>=ini_lat-1)))  | \
                        (((next_low['lon']<=ini_lon)&(next_low['lon']>=ini_lon-1)) & ((next_low['lat']<=ini_lat+1.5)&(next_low['lat']>=ini_lat-1)))]

        else: # それ以降は各低気圧トラッキングの最終位置情報を更新していく
            tmp = output.tail(n=1) # データフレームの末尾を参照する
            next_lon = tmp.iat[0,2]
            next_lat = tmp.iat[0,3]
            d2 = next_low[(((next_low['lon']>=next_lon)&(next_low['lon']<=next_lon+2))&((next_low['lat']<=next_lat+1.5)&(next_low['lat']>=next_lat-1)))  | \
                          (((next_low['lon']<=next_lon)&(next_low['lon']>=next_lon-1)) & ((next_low['lat']<=next_lat+1.5)&(next_low['lat']>=next_lat-1)))]

        if len(d2) == 0: # 追尾する低気圧がなかった場合の処理
          d1['low_id']=low_id + 1 + i
          output = pd.concat([output,d1])
          break
              
        else:
            d2 = d2[d2['hPa']==d2['hPa'].min()] # 複数ある場合は一番低い低圧部を参照する
            d2['datetime'] = j
            d2['low_id'] = low_id + 1 + i
            output = pd.concat([output,d2])
            next_low = next_low.drop(list(d2.index)) # 一度でもトラッキングとして使われた低気圧は除く
      except:
        d1['low_id']=low_id + 1 + i # もしファイルが空の場合
            
      number +=1
      next_low.to_csv(f'{OUT_DIR}{j}.csv',index=None) # ファイルの更新


# データベースへ保存
et.data_to_db(output, DB_PATH, DB_NAME)

