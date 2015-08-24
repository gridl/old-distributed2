#!/usr/bin/env python

import os
from setuptools import setup, find_packages

setup(name='distributed',
      version='0.0.1',
      description='Distributed computing with data locality',
      url='http://github.com/mrocklin/distributed',
      author='Blaze development deam',
      author_email='blaze-dev@continuum.io',
      license='BSD',
      keywords='',
      packages=find_packages(),
      install_requires=list(open('requirements.txt').read().strip().split('\n')),
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      zip_safe=False,
      scripts=[os.path.join('bin', 'dworker'),
               os.path.join('bin', 'dcenter')])
