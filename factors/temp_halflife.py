import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from statsmodels.stats.weightstats import DescrStatsW
from math import pow

engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )

# 规范输出数据
def standard_data(r):
    df1 = r.stack(level=-1, dropna=True)
    df2 = df1.reset_index().rename(columns={'sid': 'sid', 'level_0': 'trade_dt', 0: 'value'})
    df2.rename(columns={'sid': 'sid', 'level_0': 'trade_dt', 0: 'value'})
    df3 = pd.DataFrame(df2, columns=['sid', 'trade_dt', 'value'])
    return df3

starttime = '20160101'
riskfree = 'CGB1Y.WI'
sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
            FROM mercury.ashare_eod_prices WHERE TRADE_DT > '{0}'""".format(starttime)
data = pd.read_sql(sql, engine)
sql_riskfree = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
                FROM mercury.cgb_benchmark
                WHERE TRADE_DT > '{0}' AND S_INFO_WINDCODE = '{1}'""".format(starttime, riskfree)
data_riskfree = pd.read_sql(sql_riskfree, engine)
# </editor-fold>

# todo factor calculation
window  = 252
halflife = 42
# 收盘价数据展开与合并
x = pd.pivot_table(data, values='close', index='trade_dt', columns='sid')
z = pd.pivot_table(data_riskfree, values='close', index='trade_dt', columns='sid')
data1 = pd.concat([z, x], axis=1, join='outer').fillna(method='ffill')

# 收益率
data2 = data1.pct_change()

# 建立panel
dict = {'close': data1, 'return': data2}
panel = pd.Panel.from_dict(dict)


def sub_temp(s):
    return s - temp

# dastd 因子计算
temp = panel['return'][riskfree]
# r_s - r_f
panel['temp'] = panel['return'].apply(sub_temp, axis=0)

w = pow(0.5, 1.0/halflife)
def f(y):
    return w**y
weight = np.fromfunction(f,(window,))

pp = panel['temp'].tail(window)
d1 = DescrStatsW(pp,weight).std
def func(pp):
    return DescrStatsW(pp,weight).std

panel['dastd'+str(window)] = panel['temp'].rolling(window).apply(func)


# HsigmaCNE5
# hsigmacne5 = std(e_s), r_s = alpha + beta * r_i + e_s, 线性回归求解

from __future__ import print_function
import numpy as np
from scipy import stats
import statsmodels.api as sm
import matplotlib.pyplot as plt
from statsmodels.sandbox.regression.predstd import wls_prediction_std
from statsmodels.iolib.table import (SimpleTable, default_txt_fmt)

np.random.seed(1024)# OLS 回归
X2 = sm.add_constant(asset2)
X1 = asset1
results = sm.OLS(X1, X2).fit()

# 残差序列及标准差
Z = results.resid
mean = Z.mean()
std = results.mse_resid ** 0.5

nsample = 50
x = np.linspace(0, 20, nsample)
X = np.column_stack((x, (x - 5) ** 2))
X = sm.add_constant(X)
beta = [5., 0.5, -0.01]
sig = 0.5
w = np.ones(nsample)
w[nsample * 6 / 10:] = 3
y_true = np.dot(X, beta)
e = np.random.normal(size=nsample)
y = y_true + sig * w * e
X = X[:, [0, 1]]

mod_wls = sm.WLS(y, X, weights=1./w)
res_wls = mod_wls.fit()