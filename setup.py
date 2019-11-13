#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-iterable",
    version="0.0.5",
    description="Singer.io tap for extracting Iterable data",
    author="Stitch",
    url="http://github.com/singer-io/tap-iterable",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_iterable"],
    install_requires=[
        "singer-python==5.8.1",
        "requests==2.20.0"
    ],
    extras_require={
        'dev': [
            'pylint',
            'ipdb',
            'requests==2.20.0'
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
