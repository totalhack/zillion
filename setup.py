#!/usr/bin/env python
import os
from setuptools import find_packages, setup

def get_readme_text():
    f = open(os.path.join(os.path.dirname(__file__), 'README.md'))
    text = f.read()
    f.close()
    return text

setup(
    name='zillion',
    long_description=get_readme_text(),
    author='totalhack',
    maintainer='totalhack',
    version='0.0',
    classifiers=['License :: MIT License',
                 'Programming Language :: Python :: 3',
                 ],
    packages=find_packages(),
    include_package_data=True,
    )
