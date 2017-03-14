# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name = "cubeopen",
    version="0.0.1",
    description="CubeQuant",
    packages=find_packages(),
    install_requires=[
        "pymongo",
        "pandas",
        "numpy",
        "PyMySQL",
    ]
)