# -*- coding:utf8 -*-

def singleton(cls):
    instance = cls()
    instance.__call__ = lambda: instance
    return instance