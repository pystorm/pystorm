#!/usr/bin/env python
"""
Copyright 2014-2015 Parsely, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import re
import sys

from setuptools import setup, find_packages

# Get version without importing, which avoids dependency issues
def get_version():
    with open('pystorm/version.py') as version_file:
        return re.search(r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""",
                         version_file.read()).group('version')

def readme():
    ''' Returns README.rst contents as str '''
    with open('README.rst') as f:
        return f.read()


install_requires = [
    'six>=1.5',
    'simplejson>=2.2.0',
    'msgpack-python'
]

if sys.version_info.major < 3:
    install_requires.append('contextlib2')

lint_requires = [
    'pep8',
    'pyflakes'
]

tests_require = ['pytest', 'pytest-timeout']

if sys.version_info.major < 3:
    tests_require.append('mock')

dependency_links = []
setup_requires = []

setup(
    name='pystorm',
    version=get_version(),
    author='Parsely, Inc.',
    author_email='hello@parsely.com',
    url='https://github.com/pystorm/pystorm',
    description=('Battle-tested Apache Storm Multi-Lang implementation for Python.'),
    long_description=readme(),
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=setup_requires,
    extras_require={
        'test': tests_require,
        'all': install_requires + tests_require,
        'docs': ['sphinx'] + tests_require,
        'lint': lint_requires
    },
    dependency_links=dependency_links,
    zip_safe=False,
    include_package_data=True,
)
