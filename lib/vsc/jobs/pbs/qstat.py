# -*- coding: latin-1 -*-
#
# Copyright 2015-2017 Ghent University
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
qstat provides functions to transform job information in a fixed format,
where every entry is placed on a single line.

@author: Andy Georges (Ghent University)
"""
import itertools

from collections import defaultdict

PBS_EXEC_HOST = "exec_host"
PBS_CPUTIME = 'resources_used.cput'
PBS_WALLTIME = 'resources_used.walltime'


def ranges(numbers):
    """
    Taken from http://stackoverflow.com/questions/4628333/converting-a-list-of-integers-into-range-in-python

    This will convert [1,2,3,6,7,8,12,23,25,26] into [(1,3), (6,8), (12,12), (23,23), (25,26)]. Note that
    the original list need not be sorted.
    """
    for _, b in itertools.groupby(enumerate(numbers), lambda (x, y): y - x):
        b = list(b)
        yield b[0][1], b[-1][1]


def convert_to_range(numbers):
    """
    Given a set of numbers, convert this to a string that condenses the
    set into ranges, where possible.

    For example: 1,2,3,6,7,8,9,12,23,24 will become 1-3,6-9,12,23-24. Note
    that the original list need not be sorted.
    """
    rng = list()
    for (a, b) in list(ranges(sorted(numbers))):
        if a == b:
            rng.append("%d" % a)
        else:
            rng.append("%d-%d" % (a, b))

    return ",".join(rng)


def normalise_exec_host(**kwargs):
    """
    Convert a list of nodes nodeXYZ/C into nodeXYZ/C-D format.
    @keyword job: PBSQuery.job instance used to retrieve the list of nodes
    """

    job = kwargs.get('job')

    nodecores = defaultdict(set)
    nodes = job.get_nodes()

    for node in nodes:
        (name, core) = node.split("/")
        nodecores[name].add(int(core))

    s = list()
    for node in sorted(nodecores.keys()):
        range_ = convert_to_range(nodecores[node])
        s.append("%s/%s" % (node, range_))

    return ",".join(s)


def normalise_time(**kwargs):
    """
    Convert a PBS time to a time in seconds.
    @keyword time: time, string in [[[DD:]HH:]MM]:SS format.
    """
    time = kwargs.get('time')

    # workaround for the format provided by PBSQuery
    if isinstance(time, list):
        time = time[0]

    parts = [int(t) for t in time.split(":")]
    parts.reverse()

    seconds = 0
    multiplier = 1
    for t in parts[:3]:
        seconds += t * multiplier
        multiplier *= 60

    if len(parts) > 3:
        seconds += 24 * 60 * 60 * parts[3]

    return seconds


def transform_info(job, info):
    """
    Print the information in the job structure for the requested info items and reformat the data.

    @param job: PBSQuery.job instance
    @param info: The info string is a comma-separated list with items of the form input_key:output_key. The input_key
    is used to index the job dictionary structure PBSQuery provides. The output_key will be used as a prefix
    on the line that is printed with the transformed information.
    """
    transformers = {
        PBS_EXEC_HOST: normalise_exec_host,
        PBS_CPUTIME: normalise_time,
        PBS_WALLTIME: normalise_time,
    }

    keys = [k.strip().split(":") for k in info.split(",")]

    s = list()
    for (input_key, output_key) in keys:
        try:
            (component, item) = input_key.split(".")
            value = job[component][item]
        except ValueError:
            value = job[input_key]
        except KeyError:
            s.append("%s: %d" % (output_key, 0))
            continue

        try:
            transformed_value = transformers[input_key](job=job, time=value)
        except KeyError:
            transformed_value = value
        s.append("%s: %s" % (output_key, transformed_value))

    return s
