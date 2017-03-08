# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine

from factors_handler import FactorBase

# write the documentation of factor calculation
"""
因子描述：120 日 Jensen's alpha, 指数使用中证500指数，无风险利率使用一年期国债CGB1Y
数据调用：mercury.ashare_eod_prices, mercury.aindex_eod_prices, wind.CGBBENCHMARK
计算方法：股票与指数收益率协方差/单只股票收益率标准差 -> beta;
            Jensen's alpha = Portfolio Return − [Risk Free Rate + Portfolio Beta * (Market Return − Risk Free Rate)]
            alpha_J = E(r_s) - [E(r_f) + beta_s_i * (E(r_i) - E(r_f))]， 均值取120日均值
"""
class ALPHA120(FactorBase):
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
            计算单只股票日收益率，计算指数日收益率，计算一年期国债日收益率
            以股票时间为基准合并股票Dataframe与指数Dataframe
            rolling计算: 股票与指数收益率协方差/单只股票收益率标准差 -> beta;
                        Jensen's alpha = Portfolio Return − [Risk Free Rate + Portfolio Beta * (Market Return − Risk Free Rate)]
                        alpha_J = E(r_s) - [E(r_f) + beta_s_i * (E(r_i) - E(r_f))]， 均值取120日均值
            去掉多余的列，标准化列名
            """
            data1 = sd.sort_values(by='trade_dt')
            data1_index = data_index.sort_values(by='trade_dt')
            data1_riskfree = data_riskfree.sort_values(by='trade_dt')

            # 计算单只股票日收益率，计算指数日收益率，计算一年期国债日收益率
            data2 = pd.concat([data1[['sid', 'trade_dt']], data1['close_s'].pct_change().rename('return')], axis=1)
            data2_index = pd.concat(
                [data1_index[['sid', 'trade_dt']], data1_index['close_i'].pct_change().rename('return')], axis=1)
            rf_return = (1 + data1_riskfree['close_rf'] / 100) ** (1.0 / 250) - 1.0
            data2_riskfree = pd.concat([data1_riskfree[['sid', 'trade_dt']], rf_return.rename('return')], axis=1)

            # rolling计算: 股票与指数收益率协方差/单只股票收益率标准差 -> beta;
            data3 = data2.join(data2_index.set_index('trade_dt'), on='trade_dt', lsuffix='_s', rsuffix='_i')
            data3 = data3.join(data2_riskfree.set_index('trade_dt'), on='trade_dt', lsuffix='_0', rsuffix='_rf') \
                .rename(columns={'sid': 'sid_rf', 'return': 'return_rf'})
            cov = data3['return_s'].rolling(window=window).cov(other=data3['return_i'])
            std = data3[['return_s']].rolling(window=window).std().rename(columns={'return_s': 'std_s'})
            beta = cov / std['std_s']

            # rolling计算：alpha_J = E(r_s) - [E(r_f) + beta_s_i * (E(r_i) - E(r_f))]
            data4 = pd.concat([data3, beta], axis=1, ignore_index=False).rename(columns={0: 'beta'})
            alpha_J = data4['return_s'].rolling(window=window).mean() \
                      - (data4['return_rf'].rolling(window=window).mean() +
                         data4['beta'] * (data4['return_i'].rolling(window=window).mean() -
                                          data4['return_rf'].rolling(window=window).mean()))
            data5 = pd.concat([data4[['sid_s', 'trade_dt']], alpha_J], axis=1).rename(columns={0: 'alpha_J'})
            data5.rename(columns={'sid_s': 'sid'}, inplace=True)

            # todo standard date
            df = pd.concat([df, data5])

        df.rename(columns={'alpha_J': 'value'})
        return df

if __name__ == '__main__':
    engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )
    # write factor info
    factor = ALPHA120(engine, 'ALPHA120', '120 日 Jensen\'s alpha, 指数使用中证500指数，无风险利率使用一年期国债CGB1Y', 14)
    factor.update()
