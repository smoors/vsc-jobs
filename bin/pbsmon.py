#!/usr/bin/env python
#
# Copyright 2013-2017 Ghent University
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
This script displays the status of the PBS batch.
It display the status of a node.
The code is based on pbsmon from pbs_python release by Bas van der Vlies <basv@sara.nl> (also GPL)
The ideas are based on the awk-script of Willem Vermin

@author: Stijn De Weirdt (Ghent University)
"""
import math
import os
from vsc.utils import fancylogger
from vsc.jobs.pbs.nodes import ND_STATE_OK, ND_STATE_NOTOK, ND_STATE_OTHER, TRANSLATE_STATE, collect_nodeinfo

_log = fancylogger.getLogger('pbsmon')


def get_row_col():
    """Get the dimensions of current terminal in row,col"""
    row, col = 24, 80

    stty = '/usr/bin/stty'
    if os.path.exists(stty):
        try:
            row, col = [int(x) for x in os.popen('%s size 2>/dev/null' % stty).read().strip().split(' ')]
        except Exception:
            pass
    row = os.environ.get('ROWS', row)
    col = os.environ.get('COLUMNS', col)

    return row, col


def get_size(width, items, mode=None):
    """
    Return max_per_row and the format
        @param: width is width of the nodename
        @param: items total number of nodes
        @param mode plot style
    """

    _, col = get_row_col()

    if width < 2:
        width = 2

    fmt_w = "%%%ds" % width

    col_per_node = width + 1
    row_per_node = 3  # node + state

    max_num_nodes_per_row = col // col_per_node

    row_col_ratio = 1.0 * 2 / 1  # assume 1 row = 2 cols (in pixels)

    node_area = col_per_node * row_per_node * row_col_ratio  # in square cols
    tot_node_area = items * node_area

    if mode is None:
        mode = 'ratio'

    if mode == 'maxfill':
        # max number of nodes per row
        max_per_row = max_num_nodes_per_row
    elif mode == 'squarish':
        # try make it appear like a square
        max_per_row = int(math.sqrt(tot_node_area) / col_per_node) + 1
    elif mode == 'ratio':
        # try make keep the screen ratio
        screen_ratio = col / (col * row_col_ratio)
        max_per_row = int(math.sqrt(tot_node_area / screen_ratio) / col_per_node) + 1
    else:
        _log.raiseException('get_size: unknown mode %s' % mode)

    # sanity
    max_per_row = min(max_per_row, max_num_nodes_per_row)
    # leave at least 1 free col on right side
    # the whitespace at the left is garanteed
    if max_per_row * col_per_node == col:
        max_per_row -= 1

    return max_per_row, fmt_w


def display_cluster_status(nl, sl):
    """Create the ascii representation of the cluster"""
    width = len(nl[-1])
    items = len(nl)

    max_per_row, fmt_w = get_size(width, items)

    start = 0
    step = end = max_per_row

    txt = []
    while start < items:
        if end > items:
            end = items

        txt.append(' ' + ' '.join([fmt_w % (nl[j]) for j in range(start, end)]))
        txt.append(' ' + ' '.join([fmt_w % (TRANSLATE_STATE[sl[j]]) for j in range(start, end)]))
        txt.append('')  # empty line under each row
        start = end
        end += step

    print "\n".join(txt)

    # good = left = even , bad = right = odd
    # - others should be odd
    maxlen = len(ND_STATE_NOTOK) + 1  # +1 for other
    if len(ND_STATE_OK) > maxlen:
        maxlen = len(ND_STATE_OK)

    fmt_filler = "%s %%-20s   %%-3s |" % fmt_w
    fmt = "%s %%-20s : %%-3s |" % fmt_w

    filler = fmt_filler % (' ', ' ', ' ')

    stats = [[filler, filler][:] for x in range(maxlen)]  # make explicit copies
    other = sum([sl.count(key) for key in ND_STATE_OTHER])

    for idx, key in enumerate(ND_STATE_OK):
        value = TRANSLATE_STATE[key]
        stats[idx][0] = fmt % (value, [key, "full"][key == "job-exclusive"], sl.count(key))

    for idx, key in enumerate(ND_STATE_NOTOK):
        value = TRANSLATE_STATE[key]
        stats[idx][1] = fmt % (value, key, sl.count(key))

    stats[-1][1] = fmt % ('o', "other", other)

    print "\n".join([''.join(x) for x in stats])


def display_node_types(types):
    """Give an overview of all types of nodes"""
    template = "%sppn=%s, physmem=%sGB, swap=%sGB, vmem=%sGB, local disk=%sGB"
    txt = ['', 'Node type:']
    offset = ' '
    if len(types) > 1:
        txt[-1].replace(':', 's:')
        offset = " " * 2

    for typ, _ in sorted(types.items(), key=lambda x: len(x[1]), reverse=True):
        # most frequent first
        cores, phys, swap, disk = typ
        txt.append(template % (offset, cores, phys, swap, phys + swap, disk))

    print "\n".join(txt)


if __name__ == '__main__':
    node_list, state_list, types = collect_nodeinfo()
    display_cluster_status(node_list, state_list)
    display_node_types(types)
