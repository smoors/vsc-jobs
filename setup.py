#!/usr/bin/env python
# -*- coding: latin-1 -*-
# #
# Copyright 2009-2015 Ghent University
#
# This file is part of vsc-jobs,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
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
from vsc.install.shared_setup import ag, sdw, jt

VERSION = '0.18'

PACKAGE = {
    'version': VERSION,
    'author': [sdw, ag, jt],
    'maintainer': [sdw, ag, jt],
    'setup_requires': [
        'vsc-install >= 0.10.25',
    ],
    'install_requires': [
        'lxml',
        # don't use installs from pbs-python from pypi
        # use local install from https://oss.trac.surfsara.nl/pbs_python/ticket/41#attachments
        # or rpms
        # 'pbs_python >= 4.6',
        'vsc-administration >= 0.20.1',
        'vsc-accountpage-clients >= 0.1.2',
        'vsc-base >= 2.4.2',
        'vsc-config >= 1.26',
        'vsc-ldap >= 1.3.4',
        #'vsc-ldap-extension >= 1.10',
        'vsc-utils >= 1.4.6',
        'lxml',
    ],
    'tests_require': [
        'mock',
        'pbs-python >= 4.4.1',
    ],
    'dependency_links': [
        # use this old pbs_python for testing
        'git+https://github.com/ehiggs/pbs-python.git#egg=pbs-python-4.4.1',
    ],
}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
