# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine

from factors_handler import FactorBase

# todo write the documentation of factor calculation
"""
因子描述：收益的120日峰度
数据调用：mercury.ashare_eod_prices
计算方法：对当前日期之前的120个交易日（包括今日）的收益率计算峰度
"""
class KURTOSIS120(FactorBase):
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

        window = 120
        df = pd.DataFrame()
        # 对单只股票计算因子值，然后合并所有股票
        for stock, sd in data.groupby(by='sid'):
            """
            对交易日排序
            计算单只股票收益率
            rolling计算120日峰度
            去掉多余的列
            """
            data1 = sd.sort_values(by='trade_dt')
            data2 = pd.concat([data1[['sid', 'trade_dt']], data1['close'].pct_change().rename('return')], axis=1)
            data3 = data2.rolling(window, 1).kurt().rename(columns={'return': 'ret_kurtosis'})
            # todo standard date
            df = pd.concat([df, data3])

        return data

if __name__ == '__main__':
    engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )
    # write factor info
    factor = KURTOSIS120(engine, 'KURTOSIS120', '收益的120日峰度', 14)
    factor.update()
