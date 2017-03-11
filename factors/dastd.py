import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from statsmodels.stats.weightstats import DescrStatsW

from factors_handler import FactorBase

# todo write the documentation of factor calculation
"""
数据调用：mercury.ashare_eod_prices, mercury.cgb_benchmark

因子描述：DASTD252: 252 日超额收益标准差，半衰期为42日。
计算方法：dastd = std(r - r_f)
"""


# 规范输出数据
def standard_data(r):
    df1 = r.stack(level=-1, dropna=True)
    df2 = df1.reset_index().rename(columns={'sid': 'sid', 'level_0': 'trade_dt', 0: 'value'})
    df2.rename(columns={'sid': 'sid', 'level_0': 'trade_dt', 0: 'value'})
    df3 = pd.DataFrame(df2, columns=['sid', 'trade_dt', 'value'])
    return df3


class DASTD(FactorBase):
    # 添加三个变量：factor，window, halflife
    #    factor：被计算的因子
    #    window：rolling的window
    #    halflife：权重的半衰期
    def __init__(self, engine, name, description, category, factor, window, halflife):
        super().__init__(engine, name, description, category)
        self.factor = factor
        self.window = window
        self.halflife = halflife

    def get_factor_value(self):
        """执行计算（如果需要）并返回所要更新的数据，
        计算区间可以由 date_back_to 确定。

        Returns
        -------
        pd.DataFrame : 需要更新的数据，必须包含sid, trade_dt, value三列
        """
        # todo calculate factor value
        # <editor-fold desc="select data 数据选取部分">
        riskfree = 'CGB1Y.WI'
        sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
                    FROM mercury.ashare_eod_prices WHERE TRADE_DT > '{0}'""".format(self.date_back_to)
        data = pd.read_sql(sql, engine)
        sql_riskfree = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_DQ_CLOSE close
                        FROM mercury.cgb_benchmark
                        WHERE TRADE_DT > '{0}' AND S_INFO_WINDCODE = '{1}'""".format(self.date_back_to, riskfree)
        data_riskfree = pd.read_sql(sql_riskfree, engine)
        # </editor-fold>

        # todo factor calculation

        # 收盘价数据展开与合并
        x = pd.pivot_table(data, values='close', index='trade_dt', columns='sid')
        z = pd.pivot_table(data_riskfree, values='close', index='trade_dt', columns='sid')
        data1 = pd.concat([z, x], axis=1, join='outer').fillna(method='ffill')

        # 收益率
        data2 = data1.pct_change()

        # 建立panel
        dict = {'close': data1, 'return': data2}
        panel = pd.Panel.from_dict(dict)

        # tool: 用于apply，dataframe的每一列减去一个Series或只有一个colunm的dataframe
        def sub_temp(s):
            return s - temp

        temp = panel['return'][riskfree]
        # r_s - r_f
        panel['temp'] = panel['return'].apply(sub_temp, axis=0)

        # 计算半衰期权重因子
        w = pow(0.5, 1.0 / self.halflife)

        def f(y):
            return w ** y

        # 得到array形式的半衰期权重序列
        weight = np.fromfunction(f, (self.window,))

        def func(pp):
            return DescrStatsW(pp, weight).std

        # rolling apply计算dastd
        panel['dastd' + str(self.window)] = panel['temp'].rolling(self.window).apply(func)

        # 数据规整
        r = panel['dastd' + str(self.window)].copy()
        r.drop(riskfree, axis=1, inplace=True)
        df = standard_data(r)

        return df


if __name__ == '__main__':
    engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )
    # write factor info

    # dastd因子
    factor_dastd252 = DASTD(engine, 'DASTD252', '252 日超额收益标准差，' \
                                                '半衰期为42日', 14, 'VARIANCE', window=252, halflife=42)
    factor_dastd252.update()
