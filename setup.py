#!/usr/bin/env python
# coding=utf-8

from setuptools import setup, find_packages

setup(
    name='connection_pool',
    version='0.0.3',
    description='thread safe connection pool',
    long_description=open('README.rst').read(),
    author='zhouyl',
    author_email='81438567@qq.com',
    license='MIT',
    packages=find_packages(),
    url='https://github.com/zhouyl/ConnectionPool',
    classifiers=[
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        "License :: OSI Approved :: MIT License",
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)
