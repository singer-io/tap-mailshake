#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-mailshake',
      version='1.0.0',
      description='Singer.io tap for extracting data from the Mailshake API',
      author='nick.civili@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mailshake'],
      install_requires=[
          'backoff==2.2.1',
          'requests==2.33.0',
          'singer-python==6.8.0'
      ],
      entry_points='''
          [console_scripts]
          tap-mailshake=tap_mailshake:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_mailshake': [
              'schemas/*.json',
              'tests/*.py'
          ]
      })
