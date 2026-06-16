#!/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools

setuptools.setup(
    name='geoseeq',
    version='0.7.4',  # DEPRECATED see pyproject.toml
    # remember to update version string in CLI as well
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
        'xxhash',
    ],
    extras_require={
        's3': ['boto3>=1.28'],
    },
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
