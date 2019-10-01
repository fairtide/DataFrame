#!/usr/bin/env python3

from distutils.core import setup

setup(name='bson_dataframe',
      version='0.1.5',
      packages=[
          'bson_dataframe',
      ],
      install_requires=[
          'lz4>=1.8.0',
          'numpy>=1.14.0',
          'pandas>=0.24.0',
          'pyarrow>=0.14.1',
          'pymongo>=3.6.0',
      ])
