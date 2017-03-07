
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine

from factors_handler import FactorBase
"""
因子描述：6日收集派发指标（6-day Accumulation/Distribution ）。将市场分为两股收集（买入）及派发（估出）的力量。
计算方法：
1. 若当日收盘价高于昨日收盘价，则收集力量等于当日收盘价与真实低位之差。真实低位是当日低位与昨日收盘价两者中较低者。
buy = close – min(lowest, prev_close)
2. 若当日收盘价低于昨日收盘价，则派发力量等于当日收盘价与真实高位之差。真实高位是当日高位与昨日收盘价两者中较高者。
sell = close – max(highest, prev_close)
3. 将收集力量（buy，正数）及派发力量（sell，负数）相加，即可得到市场的净收集力量ACD。ACD = sum(buy) + sum(sell)。
"""
class ACD(FactorBase):
    def __init__(self, engine, name, description, category):
        super().__init__(engine, name, description, category)

    def get_factor_value(self):
        """执行计算（如果需要）并返回所要更新的数据，
        计算区间可以由 date_back_to 确定。

        Returns
        -------
        pd.DataFrame : 需要更新的数据，必须包含sid, trade_dt, value三列
        """



        sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt,
                S_DQ_PRECLOSE preclose, S_DQ_OPEN open, S_DQ_HIGH high, S_DQ_LOW low, S_DQ_
            FROM mercury.ashare_eod_derivative_indicator WHERE TRADE_DT > {0}""".format(self.date_back_to)
        data = pd.read_sql(sql, self.engine)
        # TODO 因子计算
        return data

if __name__ == '__main__':
    engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )
    factor = PE(engine, 'PE', 'value factor', 1)
    factor.update()
