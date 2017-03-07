
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine

x = pd.DataFrame()

engine = create_engine(
        "mysql+pymysql://hsquant:hs123456@218.1.122.196:3306/factors?charset=utf8&autocommit=true", echo=False
    )
sql = """SELECT S_INFO_WINDCODE sid, TRADE_DT trade_dt, S_VAL_PE value
            FROM mercury.ashare_eod_derivative_indicator WHERE TRADE_DT > {0}""".format('20170101')

data = pd.read_sql(sql, engine)

print(data)