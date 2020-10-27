#!/usr/bin/env python
from setuptools import setup

setup(
    name="ordway-tap",
    version="0.2.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["ordway_tap"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests",
        "kafka-python",
        "inflection"
    ],
    entry_points="""
    [console_scripts]
    ordway-tap=ordway_tap:main
    """,
    packages=["ordway_tap"],
    package_data = {
        "schemas": ["ordway_tap/schemas/*.json"]
    },
    include_package_data=True,
)
