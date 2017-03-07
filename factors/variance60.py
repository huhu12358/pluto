# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
from math import sqrt

from factors_handler import FactorBase
"""
因子描述：60 日收益方差。
数据调用：mercury.ashare_eod_prices
计算方法：
对当前日期之前的60个交易日（包括今日）的收益率计算方差，得到日度方差
注：因子值为年化后的值，等于日度方差*sqrt(250)。
"""

class VARIANCE60(FactorBase):
    def __init__(self, engine, name, description, category):
        super().__init__(engine, name, description, category)

    def get_factor_value(self):
        """执行计算（如果需要）并返回所要更新的数据，
        计算区间可以由 date_back_to 确定。

        Returns
        -------
        pd.DataFrame : 需要更新的数据，必须包含sid, trade_dt, value三列
        """
        # calculate factor value
        sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
                    FROM mercury.ashare_eod_prices WHERE TRADE_DT > {0}""".format(self.date_back_to)

        data = pd.read_sql(sql, self.engine)

        window = 60
        df = pd.DataFrame()
        # 对单只股票计算因子值，然后合并所有股票
        for stock, sd in data.groupby(by='sid'):
            # 对交易日排序
            data1 = sd.sort_values(by='trade_dt')
            # 计算单只股票收益率
            data2 = pd.concat([data1[['sid', 'trade_dt']], data1['close'].pct_change().rename('return')], axis=1)
            # rolling计算20日交易方差（日度方差）
            data3 = data2.rolling(window, 1).var().rename(columns={'return': 'ret_variance'})
            # 转化为年度方差
            data3['y_ret_var'] = data3['ret_variance'] * sqrt(250)
            # 去掉多余的列
            data3.drop('ret_variance', axis=1, inplace=True)
            # todo standard date
            df = pd.concat([df,data3])

        df.rename(columns={'y_ret_var':'value'})

        return data

if __name__ == '__main__':
    engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )
    # write factor info
    factor = VARIANCE60(engine, 'VAR60', '60日收益方差', 14)
    factor.update()
