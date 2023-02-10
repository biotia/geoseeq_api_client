#!/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools

setuptools.setup(
    name='geoseeq_api',
    version='0.1.0',  # remember to update version string in CLI as well
    author="David C. Danko",
    author_email='dcdanko@biotia.io',
    packages=setuptools.find_packages(),
    package_dir={'geoseeq': 'geoseeq'},
    install_requires=[
        'requests',
        'click',
        'pandas',
        'biopython',
    ],
    entry_points={
        'console_scripts': [
            'geoseeq=geoseeq.cli:main'
        ]
    },
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
    ],
)
