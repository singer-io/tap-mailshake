#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-mailshake',
      version='0.0.9',
      description='Singer.io tap for extracting data from the Mailshake API',
      author='nick.civili@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mailshake'],
      install_requires=[
          'backoff==1.10.0',
          'requests==2.32.4',
          'singer-python==5.13.2'
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
