#!/usr/bin/env python
# -*- coding: latin-1 -*-
# #
# Copyright 2009-2015 Ghent University
#
# This file is part of vsc-jobs,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
# #
"""
vsc-jobs base distribution setup.py

@author: Andy Georges (Ghent University)
"""

import vsc.install.shared_setup as shared_setup
from vsc.install.shared_setup import ag, sdw, URL_GH_HPCUGENT

import glob

VERSION = '0.11.0'

PACKAGE = {
    'name': 'vsc-jobs',
    'version': VERSION,
    'author': [sdw, ag],
    'maintainer': [sdw, ag],
    'scripts': glob.glob('bin/*.py'),
    'install_requires': [
        'vsc-administration >= 0.20.1',
        'vsc-accountpage-clients >= 0.1.2',
        'vsc-base >= 2.4.2',
        'vsc-utils >= 1.4.6',
        'pbs_python >= 4.3',
        'lxml',
    ],
}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE, urltemplate=URL_GH_HPCUGENT)
