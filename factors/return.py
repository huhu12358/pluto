# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from factors_handler import FactorBase

# todo write the documentation of factor calculation
"""
因子描述：
数据调用：
计算方法：
"""


def standard_data(r):
    df1 = r.stack(level=-1, dropna=True)
    df2 = df1.reset_index().rename(columns={'sid': 'sid', 'level_0': 'trade_dt', 0: 'value'})
    df2.rename(columns={'sid': 'sid', 'level_0': 'trade_dt', 0: 'value'})
    df3 = pd.DataFrame(df2, columns=['sid', 'trade_dt', 'value'])
    return df3


class RETURN(FactorBase):
    def __init__(self, engine, name, description, category, factor, window):
        super().__init__(engine, name, description, category)
        self.factor = factor
        self.window = window

    def get_factor_value(self):
        """执行计算（如果需要）并返回所要更新的数据，
        计算区间可以由 date_back_to 确定。

        Returns
        -------
        pd.DataFrame : 需要更新的数据，必须包含sid, trade_dt, value三列
        """
        # todo calculate factor value
        # <editor-fold desc="select data 数据选取部分">
        index = '000906.SH'
        riskfree = 'CGB1Y.WI'
        sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
                    FROM mercury.ashare_eod_prices WHERE TRADE_DT > '{0}'""".format(self.date_back_to)
        data = pd.read_sql(sql, engine)
        sql_index = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
                        FROM mercury.aindex_eod_prices
                        WHERE TRADE_DT > '{0}' AND S_INFO_WINDCODE = '{1}'""".format(self.date_back_to, index)
        data_index = pd.read_sql(sql_index, engine)
        sql_riskfree = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
                        FROM wind.CGBBENCHMARK
                        WHERE TRADE_DT > '{0}' AND S_INFO_WINDCODE = '{1}'""".format(self.date_back_to, riskfree)
        data_riskfree = pd.read_sql(sql_riskfree, engine)
        # </editor-fold>

        # todo factor calculation

        # 收盘价数据展开与合并
        remove = [index, riskfree]
        x = pd.pivot_table(data, values='close', index='trade_dt', columns='sid')
        y = pd.pivot_table(data_index, values='close', index='trade_dt', columns='sid')
        z = pd.pivot_table(data_riskfree, values='close', index='trade_dt', columns='sid')
        data1 = pd.concat([z, y, x], axis=1, join='outer').fillna(method='ffill')

        # 收益率
        data2 = data1.pct_change()

        # 建立panel
        dict = {'close': data1, 'return': data2}
        panel = pd.Panel.from_dict(dict)


        # 收益率的方差，滚动计算
        panel['ret_var' + str(self.window)] = panel['return'].rolling(self.window).var()
        if self.factor == 'VARIANCE':
            r = panel['ret_var' + str(self.window)].copy()
            r.drop(remove, axis=1, inplace=True)
            df = standard_data(r)
            return df


        # 收益率的峰度，滚动计算
        panel['ret_kurt' + str(self.window)] = panel['return'].rolling(self.window).kurt()
        # 截面去极值
        panel['ret_kurt' + str(self.window)][abs(panel['ret_kurt' + str(self.window)]) > 10e10] = np.nan
        if self.factor == 'KURTOSIS':
            r = panel['ret_kurt' + str(self.window)].copy()
            r.drop(remove, axis=1, inplace=True)
            df = standard_data(r)
            return df

        # 收益率beta，滚动计算
        # 1）收益率标准差，滚动计算
        panel['ret_std' + str(self.window)] = panel['return'].rolling(self.window).std()
        # 2）各个股票对index的协方差，滚动计算
        index_r = panel['return'][index]
        panel['ret_cov' + str(self.window)] = panel['return'].rolling(self.window).cov(other=index_r)
        # 3）BETA，滚动计算
        panel['ret_beta' + str(self.window)] = panel['ret_cov' + str(self.window)] / panel['ret_std' + str(self.window)]

        # 截面去极值
        panel['ret_beta' + str(self.window)][abs(panel['ret_beta' + str(self.window)]) > 10e10] = np.nan
        if self.factor == 'BETA':
            r = panel['ret_beta' + str(self.window)].copy()
            r.drop(remove, axis=1, inplace=True)
            df = standard_data(r)
            return df


        # 收益率alpha，滚动计算
        # alpha_J = E(r_s) - [E(r_f) + beta_s_i * (E(r_i) - E(r_f))]
        # E(r_s)
        panel['ret_mean' + str(self.window)] = panel['return'].rolling(self.window).mean()
        # E(r_f)
        riskfree_r_mean = panel['return'][riskfree].rolling(self.window).mean()
        # E(r_i)
        index_r_mean = index_r.rolling(self.window).mean()

        def mul_temp(s):
            return s * temp

        def add_index_r_mean(s):
            return s + index_r_mean

        temp = index_r_mean - riskfree_r_mean
        # beta_s_i * (E(r_i) - E(r_f))
        temp = panel['ret_beta' + str(self.window)].apply(mul_temp, axis=0)
        # E(r_f) + beta_s_i * (E(r_i) - E(r_f))
        temp = temp.apply(add_index_r_mean, axis=0)
        # # alpha_J = E(r_s) - [E(r_f) + beta_s_i * (E(r_i) - E(r_f))]
        panel['ret_alpha' + str(self.window)] = panel['ret_mean' + str(self.window)] - temp

        if self.factor == 'ALPHA':
            r = panel['ret_alpha' + str(self.window)].copy()
            r.drop(remove, axis=1, inplace=True)
            df = standard_data(r)
            return df




            #return df


if __name__ == '__main__':
    engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )
    # todo write factor info
    """
    # 方差因子
    factor_var20 = RETURN(engine, 'VARIANCE20', '20日收益方差', 14, 'VARIANCE',20)
    factor_var20.update()

    factor_var60 = RETURN(engine, 'VARIANCE20', '60日收益方差', 14, 'VARIANCE',60)
    factor_var60.update()

    factor_var120 = RETURN(engine, 'VARIANCE20', '120日收益方差', 14, 'VARIANCE',120)
    factor_var120.update()

    # 峰度因子
    factor_kurt20 = RETURN(engine, 'KURTOSIS20', '收益的20日峰度', 14, 'KURTOSIS', 20)
    factor_kurt20.update()


    factor_kurt60 = RETURN(engine, 'KURTOSIS20', '收益的60日峰度', 14, 'KURTOSIS', 60)
    factor_kurt60.update()

    factor_kurt120 = RETURN(engine, 'KURTOSIS20', '收益的120日峰度', 14, 'KURTOSIS', 120)
    factor_kurt120.update()

    # BETA因子
    factor_beta20 = RETURN(engine, 'BETA20', '20日beta值，指数使用中证800指数', 14, 'BETA', 20)
    factor_beta20.update()

    factor_beta60 = RETURN(engine, 'BETA60', '60日beta值，指数使用中证800指数', 14, 'BETA', 60)
    factor_beta20.update()

    factor_beta60 = RETURN(engine, 'BETA120', '120日beta值，指数使用中证800指数', 14, 'BETA', 120)
    factor_beta60.update()

    factor_beta252 = RETURN(engine, 'BETA252', '252日beta值，指数使用中证800指数', 14, 'BETA', 252)
    factor_beta252.update()
    """

    # ALPHA因子
    factor_alpha20 = RETURN(engine, 'ALPHA20', '20日 Jensen alpha, 指数使用中证500指数，'\
                                               '无风险利率使用一年期国债CGB1Y', 14, 'ALPHA', 20)
    factor_alpha20.update()