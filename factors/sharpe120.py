# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine

from factors_handler import FactorBase

# write the documentation of factor calculation
"""
因子描述：120 日 SharpeRatio，无风险利率使用一年期国债CGB1Y
数据调用：mercury.ashare_eod_prices, mercury.aindex_eod_prices, wind.CGBBENCHMARK
计算方法：sharpe = (E(r_s) - E(r_f))/std_s，，对式中的每一项rolling计算，window=120
"""
class SHARPERATIO120(FactorBase):
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
        benchmark = 'CGB1Y.WI'
        sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close_s
                    FROM mercury.ashare_eod_prices WHERE TRADE_DT > '{0}'""".format(self.date_back_to)
        data = pd.read_sql(sql, engine)
        sql_index = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close_i
                        FROM mercury.aindex_eod_prices
                        WHERE TRADE_DT > '{0}' AND S_INFO_WINDCODE = '{1}'""".format(self.date_back_to, index)
        data_index = pd.read_sql(sql_index, engine)
        sql_riskfree = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close_rf
                        FROM wind.CGBBENCHMARK
                        WHERE TRADE_DT > '{0}' AND S_INFO_WINDCODE = '{1}'""".format(self.date_back_to, benchmark)
        data_riskfree = pd.read_sql(sql_riskfree, engine)

        window = 120
        df = pd.DataFrame()
        # 对单只股票计算因子值，然后合并所有股票
        for stock, sd in data.groupby(by='sid'):
            """
            对交易日排序
            计算单只股票日收益率，计算一年期国债每日年化收益率
            以股票时间为基准合并股票Dataframe与国债Dataframe
            rolling计算: sharpe = (E(r_s) - E(r_f))/std_s，对式中的每一项rolling计算
            去掉多余的列，标准化列名
            """
            data1 = sd.sort_values(by='trade_dt')
            data1_riskfree = data_riskfree.sort_values(by='trade_dt')

            # 计算单只股票日收益率，计算一年期国债每日年化收益率
            data2 = pd.concat([data1[['sid', 'trade_dt']], data1['close_s'].pct_change().rename('return')], axis=1)
            data2_riskfree = pd.concat(
                [data1_riskfree[['sid', 'trade_dt']], data1_riskfree['close_rf'].rename('return') / 100], axis=1)
            data3 = data2.join(data2_riskfree.set_index('trade_dt'), on='trade_dt', lsuffix='_s', rsuffix='_rf')

            # rolling计算：sharpe = (E(r_s) - E(r_f))/std_s，对式中的每一项rolling计算
            Er_s = (1.0 + data3['return_s'].rolling(window=window).mean()) ** 250 - 1.0
            Er_f = data3['return_rf']
            std_s = data3['return_s'].rolling(window=window).std()
            sharpe = (Er_s - Er_f) / std_s

            data4 = pd.concat([data4[['sid_s', 'trade_dt']], sharpe], axis=1).rename(columns={0: 'sharperatio'})
            data4.rename(columns={'sid_s': 'sid'}, inplace=True)

            # todo standard date
            df = pd.concat([df, data4])

        df.rename(columns={'sharperatio': 'value'})
        return df

if __name__ == '__main__':
    engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )
    # write factor info
    factor = SHARPERATIO120(engine, 'SHARPERATIO120', '120 日 SharpeRatio，无风险利率使用一年期国债CGB1Y', 14)
    factor.update()
