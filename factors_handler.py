# -*- coding: utf-8 -*-

import abc
import warnings

import pandas as pd

from utils import try_catch, Logger

warnings.simplefilter('ignore')

logger = Logger(
    logger_name='pluto', file_path='./log/{0}_update.log'.format(pd.datetime.now().date())
)


class FactorBase(metaclass=abc.ABCMeta):
    """因子更新基类

    在创建任何新因子的时候，只需要继承该类，并重载 get_factor_value 方法。

    Parameters
    ----------
    engine : SQLAlchemy engine
        数据库连接
    name : str
        因子名称，需要是 MySQL 表名所支持的格式
    description : str
        因子描述
    category : int
        因子大类
    lookback : int
        更新时的回溯天数

    Attributes
    ----------
    date_back_to : str
        记录回溯日期，取已更新数据中最新时间戳的前 {lookback} 日，防止遗漏数据，YYYYmmdd 格式
    """

    def __init__(self, engine, name, description, category, lookback=2):
        self.engine = engine
        self.table_name = name.lower()
        # temp 时间暂定为20150101
        #self.date_back_to = '19491001'
        self.date_back_to = '20150101'

        # 如果因子信息表不存在，则创建因子信息表
        tables = pd.read_sql('SHOW TABLES', engine, index_col='Tables_in_factors')
        if 'factors_info' not in tables.index:
            create_info_sql = """
                CREATE TABLE IF NOT EXISTS factors_info
                (
                    fid           INT            NOT NULL AUTO_INCREMENT COMMENT '因子ID',
                    name          VARCHAR(20)    NOT NULL UNIQUE COMMENT '因子名称',
                    description   TEXT           NOT NULL COMMENT '因子描述',
                    category      INT            NOT NULL COMMENT '因子大类',
                    update_time   DATETIME       NOT NULL COMMENT '更新时间',
                    PRIMARY KEY (fid)
                ) ENGINE=InnoDB;
                """
            self.engine.execute(create_info_sql)
            logger.info('A new table factors_info has been created.')

        # 如果因子信息不存在，则插入一条记录
        check_info_sql = "SELECT name FROM factors_info WHERE name = '{0}'".format(name)
        info = pd.read_sql(check_info_sql, engine)

        if info.empty:
            insert_info_sql = """INSERT INTO factors_info(name, description, category, update_time)
                VALUES('{0}', '{1}', {2}, NOW())""".format(name, description, category)
            self.engine.execute(insert_info_sql)
            logger.info('A new factor {0} has been added.'.format(name))

        # 如果因子值表不存在，则建立因子值表
        if self.table_name in tables.index:
            get_start_time = """SELECT DISTINCT trade_dt
                FROM {0} ORDER BY trade_dt DESC LIMIT 1 OFFSET {1}""".format(self.table_name, lookback)
            dates = pd.read_sql(get_start_time, engine)['trade_dt']
            if dates.size > 0:
                self.date_back_to = dates[0]
        else:
            create_factor_sql = """
                CREATE TABLE IF NOT EXISTS {0}
                (
                    id            INT              NOT NULL AUTO_INCREMENT COMMENT '记录ID',
                    sid           VARCHAR(20)      NOT NULL COMMENT '证券代码',
                    trade_dt      VARCHAR(8)       NOT NULL COMMENT '交易日期',
                    value         DECIMAL(20, 4)   COMMENT '原始因子值',
                    update_time   DATETIME         NOT NULL COMMENT '更新时间',
                    PRIMARY KEY (id),
                    UNIQUE uc_trade_dt_sid(trade_dt, sid),
                    INDEX sid_trade_dt (sid, trade_dt)
                ) ENGINE=InnoDB;
                """.format(self.table_name)
            self.engine.execute(create_factor_sql)
            logger.info('A new table {0} has been created.'.format(self.table_name))

    @abc.abstractmethod
    def get_factor_value(self):
        """执行计算（如果需要）并返回所要更新的数据，
        计算区间可以由 date_back_to 确定。

        Returns
        -------
        pd.DataFrame : 需要更新的数据，必须包含sid, trade_dt, value三列
        """
        pass

    @try_catch(logger_error=logger)
    def update(self):
        """利用临时表插入数据。"""
        logger.info("Updating table: {0} ...".format(self.table_name))

        data = self.get_factor_value()
        data['update_time'] = pd.Timestamp.now()

        # 创建临时表
        create_temp_sql = """CREATE TEMPORARY TABLE IF NOT EXISTS {0}_temp
            (SELECT * FROM {0} LIMIT 0)""".format(self.table_name)
        self.engine.execute(create_temp_sql)

        # 清空临时表内容
        truncate_temp_sql = 'TRUNCATE {0}_temp'.format(self.table_name)
        self.engine.execute(truncate_temp_sql)

        # 插入临时表
        data.to_sql(self.table_name+'_temp', self.engine, if_exists='append', index=False)

        # 从临时表中拷贝
        copy_temp_sql = """REPLACE INTO {0} SELECT * FROM {0}_temp""".format(self.table_name)
        result = self.engine.execute(copy_temp_sql)

        logger.info('Updated {0} records of table {1}.'.format(result.rowcount, self.table_name))
