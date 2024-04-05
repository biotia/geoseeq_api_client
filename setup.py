#!/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools

setuptools.setup(
    name='geoseeq',
    version='0.5.6a7',  # DEPRECATED see pyproject.toml remember to update version string in CLI as well
    author="David C. Danko",
    author_email='dcdanko@biotia.io',
    description=open('README.md').read(),
    packages=setuptools.find_packages(),
    package_dir={'geoseeq': 'geoseeq'},
    install_requires=[
        'requests',
        'click',
        'pandas',
        'biopython',
        'tqdm',
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
