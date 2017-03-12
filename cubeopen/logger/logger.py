# -*- coding-utf8 -*-

__all__ = ["get_logger", "traceback"]

import os
import logging
import datetime
import traceback

def get_logger(name):
    # 创建log文件夹
    LOGDIR = "E:/Log"
    LOGNAME = datetime.datetime.now().strftime("%Y-%m-%d") + " " + name.upper() + ".log"
    if os.path.exists(LOGDIR) is False:
        os.mkdir(LOGDIR)
    # 设置logger
    logger = logging.getLogger(name)
    if len(logger.handlers) == 0:
        handler = logging.FileHandler(filename=os.path.join(LOGDIR, LOGNAME),
                                      mode="a")
        fmt = "[%(asctime)s][%(filename)s][%(lineno)s][%(levelname)s]: %(message)s"
        formatter = logging.Formatter(fmt,
                                      datefmt="%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    return logger
