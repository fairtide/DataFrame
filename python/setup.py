#!/usr/bin/env python3

from distutils.core import setup

setup(name='bson_dataframe',
      version='0.1.5',
      packages=[
          'bson_dataframe',
      ],
      install_requires=[
          'bson',
          'lz4',
          'numpy>=1.17.0',
          'pyarrow>=0.14.1',
      ])
