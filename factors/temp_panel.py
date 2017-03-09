
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from math import sqrt

x = pd.DataFrame()

engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )

# factors: variance20,
#factor = 'VARIANCE'
factor = 'KURTOSIS'
window = 20

def standard_data(r):
    df1 = r.stack(level=-1, dropna=True)
    df2 = df1.reset_index().rename(columns={'sid': 'sid', 'level_0': 'trade_dt', 0: 'value'})
    df2.rename(columns={'sid': 'sid', 'level_0': 'trade_dt', 0: 'value'})
    df3 = pd.DataFrame(df2, columns=['sid', 'trade_dt', 'value'])
    return df3

# todo select data
starttime = '20160101'
index = '000906.SH'
benchmark = 'CGB1Y.WI'
sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
            FROM mercury.ashare_eod_prices WHERE TRADE_DT > '{0}'""".format(starttime)
data = pd.read_sql(sql, engine)
sql_index = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
                FROM mercury.aindex_eod_prices
                WHERE TRADE_DT > '{0}' AND S_INFO_WINDCODE = '{1}'""".format(starttime, index)
data_index = pd.read_sql(sql_index, engine)
sql_riskfree = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
                FROM wind.CGBBENCHMARK
                WHERE TRADE_DT > '{0}' AND S_INFO_WINDCODE = '{1}'""".format(starttime, benchmark)
data_riskfree = pd.read_sql(sql_riskfree, engine)

# todo factor calculation
# 收盘价数据展开与合并
remove = [index,benchmark]
x = pd.pivot_table(data,values='close',index='trade_dt',columns='sid')
y = pd.pivot_table(data_index,values='close',index='trade_dt',columns='sid')
z = pd.pivot_table(data_riskfree,values='close',index='trade_dt',columns='sid')
data1 = pd.concat([z,y,x],axis=1,join='outer').fillna(method='ffill')

# 收益率
data2 = data1.pct_change()

# 建立panel
dict = {'close':data1, 'return':data2}
panel = pd.Panel.from_dict(dict)

# 收益率的方差，滚动计算
panel['ret_var'+str(window)] = panel['return'].rolling(window).var()
if factor=='VARIANCE':
    r = panel['ret_var' + str(window)].copy()
    r.drop(remove, axis=1, inplace=True)
    df = standard_data(r)
    print(df)

# 收益率的峰度，滚动计算
panel['ret_kurt'+str(window)] = panel['return'].rolling(window).kurt()

# 截面去极值
panel['ret_kurt'+str(window)][abs(panel['ret_kurt'+str(window)])>10e10]=np.nan
if factor=='KURTOSIS':
    r = panel['ret_kurt' + str(window)].copy()
    r.drop(remove, axis=1, inplace=True)
    df = standard_data(r)
    print(df)


# 收益率beta，滚动计算
# 收益率标准差
panel['ret_std'+str(window)] = panel['return'].rolling(window).std()
# 各个股票对benchmark的协方差
index = panel['return'][benchmark]
panel['ret_cov'+str(window)] = panel['return'].rolling(window).cov(other=index)
# BETA
panel['ret_beta'+str(window)] = panel['ret_cov'+str(window)]/panel['ret_std'+str(window)]

# 截面去极值
panel['ret_beta'+str(window)][abs(panel['ret_beta'+str(window)])>10e10]=np.nan
if factor=='BETA':
    r = panel['ret_beta' + str(window)].copy()
    r.drop(remove, axis=1, inplace=True)
    df = standard_data(r)
    print(df)