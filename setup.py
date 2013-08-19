#!/usr/bin/env python
# -*- coding: latin-1 -*-
# #
# Copyright 2009-2013 Ghent University
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
from vsc.install.shared_setup import ag, sdw


def remove_bdist_rpm_source_file():
    """List of files to remove from the (source) RPM."""
    return ['lib/vsc/__init__.py']


shared_setup.remove_extra_bdist_rpm_files = remove_bdist_rpm_source_file
shared_setup.SHARED_TARGET.update({
    'url': 'https://github.ugent.be/hpcugent/vsc-jobs',
    'download_url': 'https://github.ugent.be/hpcugent/vsc-jobs'
})

PACKAGE = {
    'name': 'vsc-jobs',
    'version': '0.5.9',
    'author': [sdw, ag],
    'maintainer': [sdw, ag],
    'packages': ['vsc', 'vsc.jobs', 'vsc.jobs.moab', 'vsc.jobs.pbs'],
    'namespace_packages': ['vsc'],
    'scripts': ['bin/mycheckjob.py',
                'bin/myshowq.py',
                'bin/pbsmon.py',
                'bin/show_jobs.py',
                'bin/show_mem.py',
                'bin/show_nodes.py',
                'bin/show_queues.py',
                'bin/show_stats.py',
                'bin/submitfilter.py'
                ],
    'install_requires': [
        'vsc-base >= 1.2',
        'lxml',
        'pbs_python >= 4.3',
    ],
    'release': 2,
    'provides': ['python-vsc-jobs = 0.3'],
}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
