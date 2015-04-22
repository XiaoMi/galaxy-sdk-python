# -*- coding: utf-8 -*-
import os
from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='galaxy-sdk-python',
    version="0.1.3",
    author='Xiaomi Technology Co.',
    author_email='heliangliang@xiaomi.com',
    url='http://dev.mi.com',
    description='Xiaomi SDS SDK for Python',
    long_description=read('README.md'),
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
