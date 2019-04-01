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
                     'Operating System :: OS Independent',
                 ),
                 description='Contains various web-scraping utilities used in Hydro/ResOps group at MBK Engineers',
                 url='https://github.com/MBKEngineers/collect.git',
                 packages=setuptools.find_packages(),
                 # install_requires=["beautifulsoup4==4.5.3"
                 #                   "matplotlib==3.0.3",
                 #                   "pandas==0.24.2",
                 #                   "PyPDF2==1.26.0",
                 #                   "python-dotenv-0.10.1",
                 #                   "requests==2.21.0",
                 #                   "selenium==3.8.0"],
                 zip_safe=False,
                 include_package_data=False)