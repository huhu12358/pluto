
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
from math import sqrt

x = pd.DataFrame()

engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )

# factors: variance20, like variance60, variance120
# todo select data
sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
            FROM mercury.ashare_eod_prices WHERE TRADE_DT > {0}""".format('20160101')

data = pd.read_sql(sql, engine)

# todo factor calculation
window = 20
example = '603998.SH'
data1 = data[data['sid']==example].sort_values(by='trade_dt')
data2 = pd.concat([data1[['sid','trade_dt']], data1['close'].pct_change().rename('return')],axis = 1)
data3 = data2.rolling(window,1).var().rename(columns={'return':'ret_variance'})
data3['y_ret_var'] = data3['ret_variance']*sqrt(250)
data3.drop('ret_variance',axis = 1,inplace=True)