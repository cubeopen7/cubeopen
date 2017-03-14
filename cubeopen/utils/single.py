# -*- coding:utf8 -*-

# 类单例修饰符
def singleton(cls):
    instance = cls()
    instance.__call__ = lambda: instance
    return instance
