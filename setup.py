#!/usr/bin/env python
# -*- coding: utf-8 -*-
import setuptools

requirements = [ ]

setuptools.setup(name='mbklab_scrapers',
                 author='MBK Engineers',
                 author_email='narlesky@mbkengineers.com',
                 classifiers=(
                     'Programming Language :: Python :: 3',
                     'Programming Language :: Python :: 3.6',
                     'Operating System :: OS Independent',
                 ),
                 description='Contains various web-scraping utilities used in Hydro/ResOps group at MBK Engineers',
                 url='https://github.com/narlesky/mbk-lab/collect',
                 packages=setuptools.find_packages(),
                 version='0.0.1')
