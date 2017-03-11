# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from factors_handler import FactorBase

# todo write the documentation of factor calculation
"""
数据调用：mercury.ashare_eod_prices, mercury.aindex_eod_prices, mercury.cgb_benchmark

因子描述：VARIANCE20: 20 日收益方差。类似VARIANCE20，VARIANCE60，VARIANCE120
计算方法：
对当前日期之前的20个交易日（包括今日）的收益率计算方差，得到日度方差
注：因子值为年化后的值，等于日度方差*sqrt(250)。


因子描述：KURTOSIS20：收益的20日峰度。类似KURTOSIS20，KURTOSIS60，KURTOSIS120
计算方法：对当前日期之前的20个交易日（包括今日）的收益率计算峰度


因子描述：BETA20：20 日 beta 值，指数使用中证500指数
            类似BETA20，BETA60，BETA120，BETA252
计算方法：计算股票与指数收益率协方差与单只股票收益率标准差，相除得到beta值
            beta = cov(r_s, r_i) / std(r_s)


因子描述：ALPHA20：20 日 Jensen's alpha, 指数使用中证500指数，无风险利率使用一年期国债CGB1Y
            类似ALPHA20，ALPHA60，ALPHA120
计算方法：股票与指数收益率协方差/单只股票收益率标准差 -> beta;
            Jensen's alpha = Portfolio Return − [Risk Free Rate + Portfolio Beta * (Market Return − Risk Free Rate)]
            alpha = E(r_s) - [E(r_f) + beta_s_i * (E(r_i) - E(r_f))]， 均值取20日均值


因子描述：SHARPERATIO20：20 日 SharpeRatio，无风险利率使用一年期国债CGB1Y。
            类似SHARPERATIO20，SHARPERATIO60，SHARPERATIO120
计算方法：sharpe = (E(r_s) - E(r_f))/std(r_s)


因子描述：TREYNORRATIO20：20 日 TreynorRatio，无风险利率使用一年期国债CGB1Y，beta指数使用中证500指数
            类似TREYNORRATIO20，TREYNORRATIO60，TREYNORRATIO120
计算方法：(E(r_s) - E(r_f))/beta


因子描述：INFORMATIONRATIO20：20 日 InformationRatio，指数使用中证500指数
            类似INFORMATIONRATIO20，INFORMATIONRATIO60，INFORMATIONRATIO120
计算方法：IR = E(r_s - r_i)/std(r_s - r_i)

"""
# 规范输出数据
def standard_data(r):
    df1 = r.stack(level=-1, dropna=True)
    df2 = df1.reset_index().rename(columns={'sid': 'sid', 'level_0': 'trade_dt', 0: 'value'})
    df2.rename(columns={'sid': 'sid', 'level_0': 'trade_dt', 0: 'value'})
    df3 = pd.DataFrame(df2, columns=['sid', 'trade_dt', 'value'])
    return df3


class RETURN(FactorBase):
    # 添加两个变量：factor，window
    #    factor：被计算的因子
    #    window：rolling的window
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
                        FROM mercury.cgb_benchmark
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

        """收益率的方差，滚动计算"""
        panel['ret_var' + str(self.window)] = panel['return'].rolling(self.window).var()
        if self.factor == 'VARIANCE':
            r = panel['ret_var' + str(self.window)].copy()
            r.drop(remove, axis=1, inplace=True)
            df = standard_data(r)
            return df

        """收益率的峰度，滚动计算"""
        panel['ret_kurt' + str(self.window)] = panel['return'].rolling(self.window).kurt()
        # 截面去极值
        panel['ret_kurt' + str(self.window)][abs(panel['ret_kurt' + str(self.window)]) > 10e10] = np.nan
        if self.factor == 'KURTOSIS':
            r = panel['ret_kurt' + str(self.window)].copy()
            r.drop(remove, axis=1, inplace=True)
            df = standard_data(r)
            return df

        """收益率beta，滚动计算"""
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

        """收益率alpha，滚动计算"""
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

        """收益率sharperatio，滚动计算"""
        # sharpe = (E(r_s) - E(r_f))/std_s
        def sub_temp(s):
            return s - temp

        if self.factor == 'SHARPERATIO':
            temp = riskfree_r_mean
            panel['ret_sharpe' + str(self.window)] = panel['ret_mean' + str(self.window)].apply(sub_temp, axis=0) \
                                                     / panel['ret_std' + str(self.window)]
            # 截面去极值
            panel['ret_sharpe' + str(self.window)][abs(panel['ret_sharpe' + str(self.window)]) > 10e10] = np.nan
            r = panel['ret_sharpe' + str(self.window)].copy()
            r.drop(remove, axis=1, inplace=True)
            df = standard_data(r)
            return df

        """收益率TreynorRatio，滚动计算"""
        # treynor = (E(r_s) - E(r_f))/beta
        if self.factor == 'TREYNORRATIO':
            temp = riskfree_r_mean
            panel['ret_treynor' + str(self.window)] = panel['ret_mean' + str(self.window)].apply(sub_temp, axis=0) \
                                                      / panel['ret_beta' + str(self.window)]
            # 截面去极值
            panel['ret_treynor' + str(self.window)][abs(panel['ret_treynor' + str(self.window)]) > 10e10] = np.nan
            r = panel['ret_treynor' + str(self.window)].copy()
            r.drop(remove, axis=1, inplace=True)
            df = standard_data(r)
            return df

        """收益率InformationRatio，滚动计算"""
        # IR = E(r_s - r_i)/std(r_s - r_i)
        if self.factor == 'INFORMATIONRATIO':
            temp = index_r
            # r_s - r_i
            panel['ret_temp1' + str(self.window)] = panel['return'].apply(sub_temp, axis=0)
            # IR = E(r_s - r_i)/std(r_s - r_i)
            panel['ret_ir' + str(self.window)] = panel['ret_temp1' + str(self.window)].rolling(self.window).mean() \
                                                 - panel['ret_temp1' + str(self.window)].rolling(self.window).std()
            # 截面去极值
            panel['ret_ir' + str(self.window)][abs(panel['ret_ir' + str(self.window)]) > 10e10] = np.nan

            r = panel['ret_ir' + str(self.window)].copy()
            r.drop(remove, axis=1, inplace=True)
            df = standard_data(r)
            return df



if __name__ == '__main__':
    engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )
    # todo write factor info

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


    # ALPHA因子
    factor_alpha20 = RETURN(engine, 'ALPHA20', '20日 Jensen alpha, 指数使用中证500指数，'\
                                               '无风险利率使用一年期国债CGB1Y', 14, 'ALPHA', 20)
    factor_alpha20.update()

    factor_alpha60 = RETURN(engine, 'ALPHA60', '60日 Jensen alpha, 指数使用中证500指数，'\
                                               '无风险利率使用一年期国债CGB1Y', 14, 'ALPHA', 60)
    factor_alpha60.update()

    factor_alpha120 = RETURN(engine, 'ALPHA20', '120日 Jensen alpha, 指数使用中证500指数，'\
                                               '无风险利率使用一年期国债CGB1Y', 14, 'ALPHA', 120)
    factor_alpha120.update()


    # sharperatio
    factor_sharpe20 = RETURN(engine, 'SHARPERATIO20', '20 日 SharpeRatio，' \
                                                      '无风险利率使用一年期国债CGB1Y', 14, 'SHARPERATIO', 20)
    factor_sharpe20.update()

    factor_sharpe60 = RETURN(engine, 'SHARPERATIO60', '60 日 SharpeRatio，' \
                                                      '无风险利率使用一年期国债CGB1Y', 14, 'SHARPERATIO', 60)
    factor_sharpe60.update()

    factor_sharpe120 = RETURN(engine, 'SHARPERATIO120', '120 日 SharpeRatio，' \
                                                      '无风险利率使用一年期国债CGB1Y', 14, 'SHARPERATIO', 120)
    factor_sharpe120.update()



    # treynorratio 因子
    factor_treynor20 = RETURN(engine, 'TREYNORRATIO20', '20 日 TreynorRatio，'\
                                                        '无风险利率使用一年期国债CGB1Y，'\
                                                        'beta指数使用中证500指数', 14, 'TREYNORRATIO', 20)
    factor_treynor20.update()

    factor_treynor60 = RETURN(engine, 'TREYNORRATIO20', '60 日 TreynorRatio，'\
                                                        '无风险利率使用一年期国债CGB1Y，'\
                                                        'beta指数使用中证500指数', 14, 'TREYNORRATIO', 60)
    factor_treynor60.update()

    factor_treynor120 = RETURN(engine, 'TREYNORRATIO20', '120 日 TreynorRatio，'\
                                                        '无风险利率使用一年期国债CGB1Y，'\
                                                        'beta指数使用中证500指数', 14, 'TREYNORRATIO', 120)
    factor_treynor120.update()

    # informationratio, IR 因子
    factor_IR20 = RETURN(engine, 'INFORMATIONRATIO20', '20 日 InformationRatio，' \
                                                       '指数使用中证500指数', 14, 'INFORMATIONRATIO', 20)
    factor_IR20.update()

    factor_IR60 = RETURN(engine, 'INFORMATIONRATIO20', '60 日 InformationRatio，' \
                                                       '指数使用中证500指数', 14, 'INFORMATIONRATIO', 60)
    factor_IR60.update()

    factor_IR120 = RETURN(engine, 'INFORMATIONRATIO20', '120 日 InformationRatio，' \
                                                       '指数使用中证500指数', 14, 'INFORMATIONRATIO', 120)
    factor_IR120.update()
