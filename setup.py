#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-iterable",
    version="2.0.0",
    description="Singer.io tap for extracting Iterable data",
    author="Stitch",
    url="http://github.com/singer-io/tap-iterable",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_iterable"],
    install_requires=[
        "singer-python==6.8.0",
        "requests==2.34.2"
    ],
    extras_require={
        'dev': [
            'pylint',
            'ipdb'
        ]
    },
    entry_points="""
    [console_scripts]
    tap-iterable=tap_iterable:main
    """,
    packages=["tap_iterable"],
    package_data = {
        "schemas": ["tap_iterable/schemas/*.json"]
    },
    include_package_data=True,
)
