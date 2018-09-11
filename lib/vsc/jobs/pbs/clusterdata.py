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
Module with Hydra cluster data

TODO: read this (once) from config file

@author: Stijn De Weirdt (Ghent University)
@author: Jens Timmerman (Ghent University)
@author: Ward Poelmans (Vrije Universiteit Brussel)
@author: Samuel Moors (Vrije Universiteit Brussel)
"""

import copy
import re

DEFAULT_VMEM = 2048 << 20  # minimum amount of ram in our machines.

DEFAULT_SERVER_CLUSTER = 'hydra'

DEFAULT_SERVER = "default"

# these amounts are in kilobytes as reported by pbsnodes
# availmem has to be taken from an clean idle node
# (i.e. no jobs in pbsnodes and right after reboot)
CLUSTERDATA = {
    DEFAULT_SERVER_CLUSTER: {
        # this is the default if not specified: 2GB
        'PHYSMEM': DEFAULT_VMEM,
        'TOTMEM': DEFAULT_VMEM,
        'NP_LCD': 1,
        'DEFMAXNP': 40,
    },
    'gpunode': {
        # this is the default if not specified: 2GB
        'PHYSMEM': DEFAULT_VMEM,
        'TOTMEM': DEFAULT_VMEM,
        'NP_LCD': 1,
        'DEFMAXNP': 32,
        'DEFMAXNGPU': 4,
    },
    'skylake': {
        'PHYSMEM': 196681412 << 10,  # ~187.6GB
        'TOTMEM': 197729984 << 10,  # ~188.6GB
        'AVAILMEM': 193907328 << 10,  # (1GB pagepool)
        'NP': 40,
        'NP_LCD': 20,
    },
    'ivybridge': {
        'PHYSMEM': 264114416 << 10,  # 251.9GB
        'TOTMEM': 265162988 << 10,  # 252.9GB
        'AVAILMEM': 260773208 << 10,  # 248.7GB
        'NP': 20,
        'NP_LCD': 10,
    },
    'ivybridge-kepler': {
        'PHYSMEM': 264114416 << 10,  # 251.9GB
        'TOTMEM': 265162988 << 10,  # 252.9GB
        'AVAILMEM': 260773208 << 10,  # 248.7GB
        'NP': 20,
        'NP_LCD': 10,
        'NGPU': 2,
    },
    'magnycours': {
        'PHYSMEM': 65940712 << 10,  # 62.9GB
        'TOTMEM': 66989284 << 10,  # 63.9GB
        'AVAILMEM': 63786500 << 10,  # 60.8GB
        'NP': 16,
        'NP_LCD': 4,
    },
    'broadwell': {
        'PHYSMEM': 264020188 << 10,  # 251.8GB
        'TOTMEM': 265068760 << 10,  # 252.8GB
        'AVAILMEM': 261000000 << 10,  # rough estimation
        'NP': 28,
        'NP_LCD': 14,
    },
    'broadwell-himem': {
        'PHYSMEM': 1585239396 << 10,  # GB
        'TOTMEM': 1586287968 << 10,  # GB
        'AVAILMEM': 1582000000 << 10,  # rough estimation
        'NP': 40,
        'NP_LCD': 20,
    },
    'broadwell-pascal': {
        'PHYSMEM': 264020188 << 10,  # 251.8GB
        'TOTMEM': 265068760 << 10,  # 252.8GB
        'AVAILMEM': 261000000 << 10,  # rough estimation
        'NP': 24,
        'NP_LCD': 12,
        'NGPU': 2,
    },
    'broadwell-geforce': {
        'PHYSMEM': 528296028 << 10,  # GB
        'TOTMEM': 529344600 << 10,  # GB
        'AVAILMEM': 525000000 << 10,  # rough estimation
        'NP': 32,
        'NP_LCD': 16,
        'NGPU': 4,
    },
}

# features corresponding to specific clusters
# these features are mutually exclusive
CLUSTERFEATURES = {
    'pascal': 'broadwell-pascal',
    'geforce': 'broadwell-geforce',
    'kepler': 'ivybridge-kepler',
    'himem': 'broadwell-himem',
}

GPUFEATURES = ['gpgpu', 'geforce', 'pascal', 'kepler']

CPUFEATURES = ['magnycours', 'ivybridge', 'broadwell', 'skylake']

FEATURES = ['adf', 'awery', 'enc10', 'enc3', 'enc4', 'enc8', 'enc9', 'gbonte', 'himem', 'mpi',
            'postgresql', 'public', 'qdr', 'refremov', 'sc', 'vdetours', 'intel', 'amd']

ALLFEATURES = FEATURES + GPUFEATURES + CPUFEATURES

MASTER_REGEXP = re.compile(r'(%s)' % '|'.join(CLUSTERDATA.keys()))


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


def get_cluster_maxgpus(cluster):
    """Return max gpus for a cluster"""

    c_d = get_clusterdata(cluster)
    return c_d.get('NGPU', c_d.get('DEFMAXNGPU', 0))


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
