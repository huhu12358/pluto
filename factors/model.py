# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine

from factors_handler import FactorBase

# todo write the documentation of factor calculation
"""
因子描述：
数据调用：
计算方法：
"""
class PE(FactorBase):
    def __init__(self, engine, name, description, category):
        super().__init__(engine, name, description, category)

    def get_factor_value(self):
        """执行计算（如果需要）并返回所要更新的数据，
        计算区间可以由 date_back_to 确定。

        Returns
        -------
        pd.DataFrame : 需要更新的数据，必须包含sid, trade_dt, value三列
        """
        # todo calculate factor value
        sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_VAL_PE value
            FROM mercury.ashare_eod_derivative_indicator WHERE TRADE_DT > {0}""".format(self.date_back_to)
        data = pd.read_sql(sql, self.engine)

        return data

if __name__ == '__main__':
    engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )
    # todo write factor info
    factor = PE(engine, 'PE_HU', 'value factor', 14)
    factor.update()
