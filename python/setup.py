#!/usr/bin/env python3

from distutils.core import setup

setup(name='arrow_bson',
      packages=[
          'arrow_bson',
      ],
      requires=[
          'bson',
          'lz4',
          'numpy',
          'pyarrow',
      ])
