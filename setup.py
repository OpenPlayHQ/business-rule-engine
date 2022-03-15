# -*- coding: utf-8 -*-

from setuptools import setup, find_packages  # type: ignore

setup(
    name='business_rule_engine',
    version='0.0.2',
    author='Devji Chhanga',
    author_email='devji.chhanga@openplaytech.com',
    description='Business rules engine with concurrency support based on business-rules-engine library by Manfed Keiser',
    keywords="b",
    python_requires='>= 3.6',
    packages=find_packages(exclude=("tests",)),  # type: ignore
    url="https://github.com/earthedalien/business-rule-engine",
    project_urls={
        'Source': 'https://github.com/earthedalien/business-rule-engine',
        'Tracker': 'https://github.com/earthedalien/business-rule-engine/issues',
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8"
    ],
    install_requires=[
        'formulas'
    ]
)
