
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
"""
对交易日排序
计算单只股票收益率
rolling计算20日交易方差（日度方差）
转化为年度方差
去掉多余的列
"""
window = 20
example = '603998.SH'
data1 = data[data['sid']==example].sort_values(by='trade_dt')
data2 = pd.concat([data1[['sid','trade_dt']], data1['close'].pct_change().rename('return')],axis = 1)
data3 = data2.rolling(window,1).var().rename(columns={'return':'ret_variance'})
data3['y_ret_var'] = data3['ret_variance']*sqrt(250)
data3.drop('ret_variance',axis = 1,inplace=True)
print(data3)

# factors: kurtosis20, like kurtosis60, kurtosis120
# todo select data
sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
            FROM mercury.ashare_eod_prices WHERE TRADE_DT > {0}""".format('20160101')

data = pd.read_sql(sql, engine)

# todo factor calculation
"""
对交易日排序
计算单只股票收益率
rolling计算20日峰度
去掉多余的列
"""
window = 20
example = '603998.SH'
data1 = data[data['sid']==example].sort_values(by='trade_dt')
data2 = pd.concat([data1[['sid','trade_dt']], data1['close'].pct_change().rename('return')],axis = 1)
data3 = data2.rolling(window,1).kurt().rename(columns={'return':'ret_kurtosis'})
print(data3)



# factors: beta20
# todo select data
date = '20170101'
index = '000300.SH'
sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close_s
            FROM mercury.ashare_eod_prices WHERE TRADE_DT > '{0}'""".format(date)
data = pd.read_sql(sql, engine)
sql_index = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close_i
                FROM mercury.aindex_eod_prices
                WHERE TRADE_DT > '{0}' AND S_INFO_WINDCODE = '{1}'""".format(date, index)
data_index = pd.read_sql(sql_index, engine)

# todo factor calculation
"""
对交易日排序
计算单只股票日收益率，计算指数日收益率
以股票时间为基准合并股票Dataframe与指数Dataframe
rolling计算收益率协方差与单只股票收益率标准差
计算beta值
去掉多余的列，标准化列名
"""
window = 20
example = '603998.SH'
data1 = data[data['sid']==example].sort_values(by='trade_dt')
data1_index = data_index.sort_values(by='trade_dt')
data2 = pd.concat([data1[['sid','trade_dt']], data1['close_s'].pct_change().rename('return')],axis = 1)
data2_index = pd.concat([data1_index[['sid','trade_dt']], data1_index['close_i'].pct_change().rename('return')],axis = 1)
data3 = data2.join(data2_index.set_index('trade_dt'),on='trade_dt',lsuffix='_s',rsuffix='_i')
cov = pd.DataFrame(data3[['return_s','return_i']].rolling(window,1).cov().ix[:,0,1]).rename(columns={'return_i':'cov_s_i'})
std = data3[['return_s']].rolling(window,1).std()
data4 = pd.concat([data3,cov['cov_s_i']/std['return_s']],axis=1).rename(columns = {0:'beta'})
data4.drop(['return_s','return_i','sid_i','return_i'],axis =1,inplace = True)
data4.rename(columns={'sid_s':'sid'},inplace=True)

print(data)
