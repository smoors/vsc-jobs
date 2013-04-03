#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
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
##
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
    'version': '0.3',
    'author': [sdw, ag],
    'maintainer': [sdw, ag],
    'packages': ['vsc', 'vsc.jobs', 'vsc.jobs.moab', 'vsc.jobs.pbs'],
    'namespace_packages': ['vsc'],
    'scripts': ['mycheckjob.py',
                'mydshowq.py',
                'pbsmonpy',
                'show_jobs.py',
                'show_mem.py',
                'show_nodes.py',
                'show_queues.py',
                'show_stats.py',
                'submitfilter.py'
                ],
    'install_requires': [
        'vsc-base >= 1.2',
        'lxml',
    ],
    'release': 2,
    'provides': ['python-vsc-jobs = 0.3'],
}


if __name__ == '__main__':
    vsc.utils.shared_setup.action_target(PACKAGE)
