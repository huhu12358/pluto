# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine

from factors_handler import FactorBase

# write the documentation of factor calculation
"""
因子描述：120 日 beta 值，指数使用中证500指数
数据调用：mercury.ashare_eod_prices, mercury.aindex_eod_prices
计算方法：计算股票与指数收益率协方差与单只股票收益率标准差，相除得到beta值
"""
class BETA120(FactorBase):
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
        index = '000906.SH'
        sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close_s
                    FROM mercury.ashare_eod_prices WHERE TRADE_DT > '{0}'""".format(self.date_back_to)
        data = pd.read_sql(sql, engine)
        sql_index = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close_i
                        FROM mercury.aindex_eod_prices
                        WHERE TRADE_DT > '{0}' AND S_INFO_WINDCODE = '{1}'""".format(self.date_back_to, index)
        data_index = pd.read_sql(sql_index, engine)

        window = 120
        df = pd.DataFrame()
        # 对单只股票计算因子值，然后合并所有股票
        for stock, sd in data.groupby(by='sid'):
            """
            对交易日排序
            计算单只股票日收益率，计算指数日收益率
            以股票时间为基准合并股票Dataframe与指数Dataframe
            rolling计算股票与指数收益率协方差与单只股票收益率标准差
            计算beta值，合并数据
            去掉多余的列，标准化列名
            """
            data1       = sd.sort_values(by='trade_dt')
            data1_index = data_index.sort_values(by='trade_dt')
            data2       = pd.concat([data1[['sid', 'trade_dt']], data1['close_s'].pct_change().rename('return')], axis=1)
            data2_index = pd.concat(
                [data1_index[['sid', 'trade_dt']], data1_index['close_i'].pct_change().rename('return')], axis=1)
            # 以股票时间为基准合并股票Dataframe与指数Dataframe
            data3       = data2.join(data2_index.set_index('trade_dt'), on='trade_dt', lsuffix='_s', rsuffix='_i')
            # rolling计算收益率协方差与单只股票收益率标准差
            cov = pd.DataFrame(data3[['return_s', 'return_i']].rolling(window, 1).cov().ix[:, 0, 1]).rename(
                columns={'return_i': 'cov_s_i'})
            std = data3[['return_s']].rolling(window, 1).std()
            #计算beta值，合并数据
            data4 = pd.concat([data3, cov['cov_s_i'] / std['return_s']], axis=1).rename(columns={0: 'beta'})
            data4.drop(['return_s', 'return_i', 'sid_i', 'return_i'], axis=1, inplace=True)
            data4.rename(columns={'sid_s': 'sid'}, inplace=True)
            # todo standard date
            df = pd.concat([df, data4])

        df.rename(columns={'beta': 'value'})
        return df

if __name__ == '__main__':
    engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )
    # write factor info
    factor = BETA120(engine, 'BETA120', '120日beta值，指数使用中证800指数', 14)
    factor.update()
