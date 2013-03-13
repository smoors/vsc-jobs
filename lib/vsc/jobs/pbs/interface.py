#!/usr/bin/env python
# #
# Copyright 2013-2013 Ghent University
#
# This file is part of vsc-base,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# http://github.com/hpcugent/vsc-base
#
# vsc-base is free software: you can redistribute it and/or modify
# it under the terms of the GNU Library General Public License as
# published by the Free Software Foundation, either version 2 of
# the License, or (at your option) any later version.
#
# vsc-base is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public License
# along with vsc-base. If not, see <http://www.gnu.org/licenses/>.
# #
"""
The main pbs module

@author: Stijn De Weirdt (Ghent University)
"""
import re
from math import ceil
from vsc import fancylogger

_log = fancylogger.getLogger('pbs.pbs', fname=False)

HAVE_PBS_PYTHON = True
try:
    import pbs
    from PBSQuery import PBSQuery
except:
    _log.error('Failed to import pbs_python modules')
    HAVE_PBS_PYTHON = False

ND_free = pbs.ND_free
ND_down = pbs.ND_down
ND_offline = pbs.ND_offline
ND_reserve = pbs.ND_reserve
ND_job_exclusive = pbs.ND_job_exclusive
ND_job_sharing = pbs.ND_job_sharing
ND_busy = pbs.ND_busy
ND_state_unknown = pbs.ND_state_unknown
ND_timeshared = pbs.ND_timeshared
ND_cluster = pbs.ND_cluster
ND_free_and_job = pbs.ND_free_and_job
ND_down_on_error = pbs.ND_down_on_error
ND_free_and_job = 'partial'
ND_down_on_error = 'downerror'

TRANSLATE_STATE = {
                   ND_free: '_',
                   ND_down: 'X',
                   ND_offline: '.',
                   ND_reserve: 'R',
                   ND_job_exclusive: 'J',
                   ND_job_sharing: 'S',
                   ND_busy: '*',
                   ND_state_unknown: '?',
                   ND_timeshared: 'T',
                   ND_cluster: 'C',
                   ND_free_and_job: 'j',
                   ND_down_on_error: 'x',
}

ND_STATE_EXOTIC = [
                   ND_busy,
                   ND_reserve,
                   ND_timeshared,
                   ND_job_sharing,
                   ND_state_unknown,
                   ND_cluster,
                   ]

ND_STATE_OK = [
               ND_job_exclusive,
               ND_free_and_job,
               ND_free,
               ]

ND_STATE_NOTOK = [
                  ND_offline,
                  ND_down,
                  ND_down_on_error,
                  ]

UNIT_PREFIX = ['', 'k', 'm', 'g', 't']
UNITS_LOWER = ['%sb' % x for x in UNIT_PREFIX]
UNIT_REG = re.compile(r'^\s*(?P<value>\d+)?(?P<unit>%s)?\s*$' % '|'.join(UNITS_LOWER), re.I)

def str2byte(txt):
    """Simple conversion of string to integer as per units used in pbs"""
    r = UNIT_REG.search(txt)
    if r is None:
        return None

    value = r.group('value')
    if value is None:
        value = 1
    else:
        value = int(value)

    unit = r.group('unit')
    if unit is None:
        unit = 'b'
    unit_int = 1024 ** UNITS_LOWER.index(unit.lower())

    return value * unit_int


def get_nodes_dict():
    """Get the pbs_nodes equivalent info as dict"""
    query = PBSQuery()
    node_states = query.getnodes([])
    for full_state in node_states.values():
        state = full_state['state'][0]  # insert additional state
        if state == ND_free:
            if 'jobs' in full_state:
                full_state['state'].insert(0, ND_free_and_job)
        elif state == pbs.ND_down:
            if 'error' in full_state:
                full_state['state'].insert(0, ND_down_on_error)

    return node_states


def get_nodes():
    """Get the pbs_nodes equivalent, return sorted list of tuples (sorted on nodename)"""
    node_states = get_nodes_dict().items()
    node_states.sort(key=lambda x: x[0])
    return node_states


def collect_nodeinfo():
    """Collect node information"""
    types = {}
    state_list = []
    node_list = []
    re_host_id = re.compile(r"(?P<id>\d+)")

    for idx, (node, full_state) in enumerate(get_nodes()):
        # A node can have serveral states. We are only interested in first entry.
        state = full_state['state'][0]
        state_list.append(state)

        if not state in ND_STATE_NOTOK:
            if 'status' in full_state and 'np' in full_state:
                # collect detailed info of the nodes
                cores = full_state['np'][0]
                physmem_txt = full_state['status'].get('physmem', [None])[0]
                totmem_txt = full_state['status'].get('totmem', [None])[0]
                size_txt = full_state['status'].get('size', [None])[0]
                if physmem_txt and totmem_txt and size_txt:
                    # # 'physmem': ['66103784kb']
                    p = str2byte(physmem_txt)
                    # # 'totmem': ['82891700kb']
                    t = str2byte(totmem_txt)
                    # # 'size': ['539214180kb:539416640kb']
                    # # - use 2nd field
                    s = str2byte(size_txt.split(':')[1])

                    # # round mem to 1 gb, size to 5gb
                    GB = str2byte('gb')
                    pmem = ceil(10 * p / GB) / 10
                    tmem = ceil(10 * t / GB) / 10
                    swap = tmem - pmem
                    dsize = ceil(10 * s / (5 * GB)) / 2
                    typ = (cores, pmem, swap, dsize)
                    if not typ in types:
                        types[typ] = 0
                    types[typ] += 1

        result = re_host_id.search(node)
        if result:
            node_list.append(result.group('id'))
        else:
            node_list.append(str(idx + 1))  # offset +1

    return node_list, state_list, types


def get_queues():
    """Get the queues"""
    query = PBSQuery()
    queues = query.getqueues()
    return queues


def get_queues_dict():
    """Get dict with queues, separated on 'disabled', 'route', 'enabled'"""
    queues_dict = {
                   'enabled': [],
                   'route': [],
                   'disabled': [],
                   }

    for name, queue in get_queues().items():
        if not queue.is_enabled():
            queues_dict['disabled'].append((name, queue))
        elif queue['queue_type'][0].lower() == 'route':
            queues_dict['route'].append((name, queue))
        else:
            queues_dict['enabled'].append((name, queue))

    return queues_dict


