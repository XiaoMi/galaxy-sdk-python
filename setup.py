# -*- coding: utf-8 -*-
import os
from setuptools import setup, find_packages

def load_file(name):
    with open(name) as fs:
        content = fs.read().strip().strip('\n')
    return content

README = load_file('README.md')

setup(
    name='galaxy-sdk-python',
    version="0.2.8",
    author='Xiaomi Technology Co.',
    author_email='heliangliang@xiaomi.com',
    url='http://dev.mi.com',
    description='Xiaomi Galaxy SDK for Python',
    long_description=README,
    packages=find_packages('lib'),
    package_dir={'': 'lib'},
    classifiers=[
        'Development Status :: 4 - Beta',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
    ],
    license='Apache License (2.0)',
)
