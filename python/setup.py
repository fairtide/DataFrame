#!/usr/bin/env python3

from distutils.core import setup

setup(name='arrow_bson',
      packages=[
          'arrow_bson',
      ],
      install_requires=[
          'bson',
          'lz4',
          'numpy>=1.17.0',
          'pyarrow>=0.14.1',
      ])
