#!/usr/bin/env python

from setuptools import setup

setup(name='tap-klaviyo',
      version='1.1.1',
      description='Singer.io tap for extracting data from the Klaviyo API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_klaviyo'],
      install_requires=['singer-python==6.0.0',
                        'requests==2.32.3'],
      entry_points='''
          [console_scripts]
          tap-klaviyo=tap_klaviyo:main
      ''',
      packages=['tap_klaviyo'],
      package_data={
          'tap_klaviyo/schemas': [
                "schemas/*.json",
                "schemas/shared/*.json"
              ]
         },
      include_package_data=True
)
