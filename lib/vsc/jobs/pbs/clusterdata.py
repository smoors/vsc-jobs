#
# Copyright 2015-2018 Ghent University
#
# This file is part of vsc-jobs,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
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
# availmem has to be taken from an clean idle node
# (i.e. no jobs in pbsnodes and right after reboot)
CLUSTERDATA = {
    'delcatty': {
        'PHYSMEM': 66045320 << 10,  # ~62.9GB
        'TOTMEM': 87016832 << 10,  # ~82.9GB
        'AVAILMEM': 84240480 << 10, # (1GB pagepool)
        'NP': 16,
        'NP_LCD': 4,
        },
    'raichu': {
        'PHYSMEM': 32973320 << 10,  # 31.4GB
        'TOTMEM': 53944832 << 10,  # 51.4GB
        'AVAILMEM': 53256304 << 10, # no pagepool
        'NP': 16,
        'NP_LCD': 4,
        },
    'muk': {
        'PHYSMEM': 66068964 << 10,  # 63.0GB
        'TOTMEM': 99623388 << 10,  # 95.0GB
        'AVAILMEM': 84683080 << 10, # (1GB pagepool)
        'NP': 16,
        'NP_LCD': 4,
        },
    'phanpy': {
        'PHYSMEM': 528271212 << 10,  # 503.7 GB
        'TOTMEM':  549242728 << 10,  # 523.7 GB
        'AVAILMEM': 528456608 << 10, # 504.0 GB (16GB pagepool)
        'NP': 24,
        'NP_LCD': 3,
    },
    'golett': {
        'PHYSMEM': 65850124 << 10,
        'TOTMEM':  86821640 << 10,
        'AVAILMEM': 84123328 << 10, # 80.2GB (1GB pagepool)
        'NP': 24,
        'NP_LCD': 3,
    },
    'shuppet': {
        'PHYSMEM': 4056736 << 10,
        'TOTMEM':  12445340 << 10,
        'NP': 2,
        'NP_LCD': 2,
    },
    'swalot': {
        'PHYSMEM': 131791292 << 10,
        'TOTMEM': 152762808 << 10,
        'AVAILMEM': 150549544 << 10, # (1GB pagepool)
        'NP': 20,
        'NP_LCD': 5,
    },
    'skitty': {
        'PHYSMEM': 196682268 << 10,
        'TOTMEM': 196682268 << 10,
        'AVAILMEM': 194682268 << 10, # (1GB pagepool)
        'NP': 36,
        'NP_LCD': 9,
    },
    'victini': {
        'PHYSMEM': 98341134 << 10,
        'TOTMEM': 98341134 << 10,
        'AVAILMEM': 98341134 << 10, # (no pagepool)
        'NP': 36,
        'NP_LCD': 9,
    },
    'banette': {
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


def get_cluster_overhead(cluster):
    """
    Return amount of unusable memory in bytes.
    This is the difference between totmem and initial availmem.
    """
    c_d = get_clusterdata(cluster)
    if 'AVAILMEM' in c_d:
        overhead = c_d['TOTMEM'] - c_d['AVAILMEM']
    else:
        overhead = 0

    return overhead


def get_cluster_mpp(cluster):
    """
    Return mpp (mem per processing unit):
        tuple with ppp (physmem) and vpp (vmem)

    Values are corrected for systemoverhead using availmem (if defined for cluster)
    """

    c_d = get_clusterdata(cluster)
    maxppn = get_cluster_maxppn(cluster)

    overhead = get_cluster_overhead(cluster)

    physmem = c_d['PHYSMEM'] - overhead
    totmem = c_d['TOTMEM'] - overhead

    ppp = int(physmem / maxppn)
    vpp = int((physmem + (totmem - physmem) / 2) / maxppn)

    return (ppp, vpp)
