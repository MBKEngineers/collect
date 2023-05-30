#!/usr/bin/env python
# -*- coding: utf-8 -*-
import setuptools


setuptools.setup(name='collect',
                 version='0.0.1',
                 author='MBK Engineers',
                 author_email='narlesky@mbkengineers.com',
                 classifiers=(
                     'Programming Language :: Python :: 3',
                     'Programming Language :: Python :: 3.6',
                     'Programming Language :: Python :: 3.8',
                     'Operating System :: OS Independent',
                 ),
                 description='Contains various web-scraping utilities used in Hydro/ResOps group at MBK Engineers',
                 url='https://github.com/MBKEngineers/collect.git',
                 packages=setuptools.find_packages(),
                 setup_requires=['numpy>=1.21.3'],
                 install_requires=['beautifulsoup4==4.5.3',
                                   'lxml==4.4.1',
                                   'pandas==1.3.4',
                                   'pdftotext==2.2.2',
                                   'python-dateutil==2.8.2',
                                   'python-dotenv==0.19.2',
                                   'requests>=2.26.0',
                                   'scipy>=1.8.0',
                                   'selenium==3.8.0',
                                   'tabula-py==2.4.0'],
                 extras_require={'docs': ['Sphinx==4.3.0', 
                                          'sphinx-readable-theme==1.3.0', 
                                          'sphinx-rtd-theme==1.0.0']},
                 zip_safe=False,
                 include_package_data=False,
                 scripts=['bin/collect-start'],
                )