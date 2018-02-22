#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function

import os
import sys

from setuptools import find_packages
from setuptools import setup

v = sys.version_info
if v[:2] < (3,3):
    error = "ERROR: Jupyter Hub requires Python version 3.3 or above."
    print(error, file=sys.stderr)
    sys.exit(1)


if os.name in ('nt', 'dos'):
    error = "ERROR: Windows is not supported"
    print(error, file=sys.stderr)

# At least we're on the python version we need, move on.

pjoin = os.path.join
here = os.path.abspath(pjoin(os.path.dirname(__file__), 'src/'))

# Get the current package version.
version_ns = {}
with open(pjoin(here, 'mesos_spawner', '_version.py')) as f:
    exec(f.read(), {}, version_ns)

install_requires = []
with open('requirements.txt') as f:
    for line in f.readlines():
        req = line.strip()
        if not req or req.startswith('#'):
            continue
        install_requires.append(req)

from setuptools.command.bdist_egg import bdist_egg
class bdist_egg_disabled(bdist_egg):
    """Disabled version of bdist_egg
    Prevents setup.py install from performing setuptools' default easy_install,
    which it should never ever do.
    """
    def run(self):
        sys.exit("Aborting implicit building of eggs. Use `pip install .` to install from source.")

setup_args = dict(
    name                = 'mesos_spawner',
    packages            = find_packages('src'),
    package_dir         = {'': 'src'},
    version             = version_ns['__version__'],
    description         = """Mesos Spawner: A Mesos framework for Jupyterhub.""",
    long_description    = "Spawn single-user servers with Mesos.",
    author              = "Tim Anderegg",
    author_email        = "timothy.anderegg@gmail.com",
    url                 = "https://github.com/tanderegg/mesos-spawner",
    license             = "MIT",
    platforms           = "Linux, Mac OS X",
    keywords            = ['Interactive', 'Interpreter', 'Shell', 'Web'],
    classifiers         = [
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
    install_requires = install_requires,
    cmdclass = {
        'bdist_egg': bdist_egg if 'bdist_egg' in sys.argv else bdist_egg_disabled,
    }
)


def main():
    setup(**setup_args)

if __name__ == '__main__':
    main()
