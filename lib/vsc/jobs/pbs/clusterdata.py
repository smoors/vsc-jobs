#
# Copyright 2015-2016 Ghent University
#
# This file is part of vsc-jobs,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-jobs
#
# vsc-jobs is free software: you can redistribute it and/or modify
# it under the terms of the GNU Library General Public License as
# published by the Free Software Foundation, either version 2 of
# the License, or (at your option) any later version.
#
# vsc-jobs is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public License
# along with vsc-jobs. If not, see <http://www.gnu.org/licenses/>.
#
"""
Module with UGent cluster data

TODO: read this (once) from config file

@author: Stijn De Weirdt (Ghent University)
@author: Jens Timmerman (Ghent University)
"""

import copy
import re

MIN_VMEM = 1536 << 20  # minimum amount of ram in our machines.

DEFAULT_SERVER_CLUSTER = 'delcatty'

DEFAULT_SERVER = "default"

MASTER_REGEXP = re.compile(r'master[^.]*\.([^.]+)\.(?:[^.]+\.vsc|os)$')

# these amounts are in kilobytes as reported by pbsnodes
CLUSTERDATA = {
    'delcatty': {
        'PHYSMEM': 66045320 << 10,  # ~62.9GB
        'TOTMEM': 87016832 << 10,  # ~82.9GB
        'NP': 16,
        'NP_LCD': 4,
        },
    'raichu': {
        'PHYSMEM': 32973320 << 10,  # 31.4GB
        'TOTMEM': 53944832 << 10,  # 51.4GB
        'NP': 16,
        'NP_LCD': 4,
        },
    'muk': {
        'PHYSMEM': 66068964 << 10,  # 63.0GB
        'TOTMEM': 99623388 << 10,  # 95.0GB
        'NP': 16,
        'NP_LCD': 4,
        },
    'phanpy': {
        'PHYSMEM': 483 << 30,  # 16GB reserved for pagepool + 4GB for other services
        'TOTMEM':  483 << 30,  # no swap on phanpy
        'NP': 24,
        'NP_LCD': 3,
    },
    'golett': {
        'PHYSMEM': 65850124 << 10,
        'TOTMEM':  86821640 << 10,
        'NP': 24,
        'NP_LCD': 3,
    },
    'shuppet': {
        'PHYSMEM': 4056736 << 10,
        'TOTMEM':  12445340 << 10,
        'NP': 2,
        'NP_LCD': 2,
    },
}


def get_clusterdata(name, make_copy=True):
    """
    Return dict with clusterdata for cluster with name

    If make_copy, return a (deep)copy of the data
    """

    data = CLUSTERDATA.get(name, CLUSTERDATA.get(DEFAULT_SERVER_CLUSTER))

    if make_copy:
        return copy.deepcopy(data)
    else:
        return data


def get_cluster_maxppn(cluster):
    """Return max ppn for a cluster"""

    c_d = get_clusterdata(cluster)
    return c_d.get('NP', c_d.get('DEFMAXNP', 1))


def get_cluster_mpp(cluster):
    """
    Return mpp (mem per processing unit):
        tuple with ppp (physmem) and vpp (vmem)
    """

    c_d = get_clusterdata(cluster)
    maxppn = get_cluster_maxppn(cluster)

    ppp = int(c_d['PHYSMEM'] / maxppn)
    vpp = int((c_d['PHYSMEM'] + (c_d['TOTMEM'] - c_d['PHYSMEM']) / 2) / maxppn)

    return (ppp, vpp)
