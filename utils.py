# -*- coding: utf-8 -*-

import sys
import os
import logging
from logging.handlers import RotatingFileHandler



def try_catch(logger_error):
    """
    记录错误修饰器

    Parameters
    ----------
    logger_error
        Logger name

    Returns
    -------

    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
                logger_error.info("Succeed!")
            except Exception as ex:
                logger_error.error(ex.__str__())
        return wrapper
    return decorator


class Logger:
    def __init__(self, logger_name, file_path,  f_handler=RotatingFileHandler,
                 clevel=logging.DEBUG, Flevel=logging.DEBUG,
                 fmt=logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
                 ):
        if not os.path.exists('./log'):
            os.mkdir('./log')
        self.logger = logging.getLogger(logger_name)
        if not len(self.logger.handlers):
            self.logger.setLevel(logging.DEBUG)

            # 设置CMD日志
            sh = logging.StreamHandler(sys.stdout)
            sh.setFormatter(fmt)
            sh.setLevel(clevel)

            # 设置文件日志
            args = (file_path, 'a', 200000000, 5, 'utf-8')
            fh = f_handler(*args)
            fh.setFormatter(fmt)
            fh.setLevel(Flevel)

            self.logger.addHandler(fh)
            self.logger.addHandler(sh)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)