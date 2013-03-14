#!/usr/bin/python
#
# Author: Bas van der Vlies <basv@sara.nl>
# Date  : 17 Aug 2001
# Desc. : This script displays the status of the PBS batch.
#         It display the status of a node. The ideas are
#         based on the awk-script of Willem Vermin
#
# CVS info:
# $Id: pbsmon.py 134 2006-10-10 09:29:08Z bas $
#
# SDW: add cluster node types
#
from vsc import fancylogger
from vsc.jobs.pbs.nodes import ND_STATE_OK, ND_STATE_NOTOK, ND_STATE_EXOTIC, TRANSLATE_STATE, collect_nodeinfo

_log = fancylogger.getLogeer('pbsmon')


def display_cluster_status(nl, sl):
    # Thanks to Daniel Olson, we have now code that can handle
    # 2 and 3 digit hostname numbers
    #
    width = len(nl[-1])

    # Determine what format we have to use
    if width == 3:
        step = end = 19
        fmt = '%3s'
    elif width < 3:
        step = end = 25
        fmt = '%2s'
    else:
        _log.error('Unsupported width %s' % width)

    start = 0
    items = len(nl)

    txt = []
    while start < items:
        if end > items:
            end = items

        txt.append(' ' + ''.join([fmt % (nl[j]) for j in range(start, end)]))
        txt.append(' ' + ''.join([fmt % (sl[j]) for j in range(start, end)]))

        start = end
        end += step

    print "\n".join(txt)

    # good = left = even , bad = right = odd
    # - others should be odd
    maxlen = len(ND_STATE_NOTOK) + 1  # +1 for other
    if len(ND_STATE_OK) > maxlen:
        maxlen = len(ND_STATE_OK)

    fmt = "%3s %-21s : %s\t |"
    filler = fmt % (' ', ' ', ' ')

    stats = [[filler] * 2] * maxlen
    other = sum([sl.count(TRANSLATE_STATE[key]) for key in ND_STATE_EXOTIC])

    for idx, key in enumerate(ND_STATE_OK):
        value = TRANSLATE_STATE[key]
        stats[idx][0] = fmt % (value, [key, "full"][key == "job-exclusive"], sl.count(value))

    for idx, key in enumerate(ND_STATE_NOTOK):
        value = TRANSLATE_STATE[key]
        stats[idx][0] = fmt % (value, key, sl.count(value))

    stats[-1][1] = fmt % ('o', "other", other),

    print "\n".join([''.join(x) for x in stats])


def display_node_types(types):
    """Give an overview of all types of nodes"""
    template = "%sppn=%s, physmem=%sGB, swap=%sGB, vmem=%sGB, local disk=%sGB"
    txt = ['', '', 'Node type:']
    offset = ' '
    if len(types) > 1:
        txt[-1].replace(':', 's:')
        offset = " " * 2

    for typ, freq in sorted(types.items(), key=lambda x: x[1], reverse=True):
        # most frequent first
        cores, phys, swap, disk = typ
        txt.append(template % (offset, cores, phys, swap, phys + swap, disk))

    print "\n".join(txt)


if __name__ == '__main__':
    node_list, state_list, types = collect_nodeinfo()
    display_cluster_status(node_list, state_list)
    display_node_types(types)
