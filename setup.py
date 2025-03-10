# -*- coding: utf-8 -*-
import setuptools


setuptools.setup(
    name='collect',
    version='0.0.2',
    author='MBK Engineers',
    author_email='narlesky@mbkengineers.com',
    classifiers=(
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.11',
        'Operating System :: OS Independent',
    ),
    description='Contains various web-scraping utilities used at MBK Engineers',
    url='https://github.com/MBKEngineers/collect.git',
    packages=setuptools.find_packages(),
    setup_requires=[
        'numpy==1.26.2'
    ],
    install_requires=[
        'beautifulsoup4==4.12.3',
        'pandas==1.5.3',
        'pyOpenSSL==23.3.0',
        'python-dateutil==2.9.0',
        'python-dotenv==0.19.2',
        'requests==2.32.3',
        'selenium==4.15.2',
        'tabula-py==2.10.0'
    ],
    extras_require={
        'docs': [
            'Sphinx==4.3.0', 
            'sphinx-readable-theme==1.3.0', 
            'sphinx-rtd-theme==1.0.0'
        ],
        'filters': 'scipy==1.10.1',
        'swp': 'pdftotext==2.2.2'
    },
    zip_safe=False,
    include_package_data=False,
    scripts=[
        'bin/collect-start'
    ]
)