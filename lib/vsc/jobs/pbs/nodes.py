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
from vsc.utils.missing import all
from vsc.jobs.pbs.interface import get_query, pbs
from vsc.jobs.pbs.tools import str2byte

_log = fancylogger.getLogger('pbs.nodes', fname=False)


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
ND_down_on_error = pbs.ND_down_on_error
ND_free_and_job = 'partial'
ND_error = 'error'
ND_down_on_error = 'downerror'
ND_bad = 'bad'

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
                   ND_error: 'e',
                   ND_down_on_error: 'x',
                   ND_bad: 'b',
}

NDST_OK = 'ok'
NDST_NOTOK = 'notok'
NDST_OTHER = 'other'

# other is all not ok or notok
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

# node states for nagios WARNING is all not critical or ok
NDNAG_CRITICAL = 'CRITICAL'
NDNAG_WARNING = 'WARNING'
NDNAG_OK = 'OK'

ND_NAGIOS_CRITICAL = [
                      ND_down,
                      ND_down_on_error,
                      ND_error,
                      ND_bad,
                      ]

ND_NAGIOS_OK = [
                ND_job_exclusive,
                ND_free_and_job,
                ND_free,
               ]

ND_NAGIOS_WARNING = [x for x in TRANSLATE_STATE.keys() if not x in ND_NAGIOS_CRITICAL + ND_NAGIOS_OK]


JOBID_REG = re.compile(r"\w+/\w+(\.|\w|\[|\])+")


def get_nodes_dict():
    """Get the pbs_nodes equivalent info as dict"""
    query = get_query()
    node_states = query.getnodes([])
    for full_state in node_states.values():
        # just add states
        states = full_state['state']
        if ND_free in states and 'jobs' in full_state:
            states.insert(0, ND_free_and_job)

        if 'error' in full_state:
            states.insert(0, ND_error)
        if ND_down in states and 'error' in full_state:
            states.insert(0, ND_down_on_error)

        if 'jobs' in full_state and not all([JOBID_REG.search(x.strip()) for x in full_state['jobs']]):
            states.insert(0, ND_bad)

        # extend the node dict with derived dict (for convenience)
        derived = {}

        derived['states'] = states

        # what state to report?
        nd_not_ok = [x for x in ND_STATE_NOTOK if x in states]
        nd_ok = [x for x in ND_STATE_OK if x in states]
        if len(nd_not_ok) > 0:
            state = nd_not_ok[0]
            ndst = NDST_NOTOK
        elif (len(nd_ok)):
            state = nd_ok[0]
            ndst = NDST_OK
        else:
            state = states[0]
            ndst = NDST_OTHER
        derived['state'] = state
        derived['nodestate'] = ndst

        # what nagios state?
        nag_crit = [x for x in ND_NAGIOS_CRITICAL if x in states]
        nag_warn = [x for x in ND_NAGIOS_WARNING if x in states]
        if len(nag_crit) > 0:
            ndnag = NDNAG_CRITICAL
        elif len(len(nag_warn)) > 0:
            ndnag = NDNAG_WARNING
        else:
            ndnag = NDNAG_OK
        derived['nagiosstate'] = ndnag

        if 'np' in full_state:
            derived['np'] = int(full_state['np'][0])
        if 'status' in full_state:
            status = full_state['status']
            for prop in ['physmem', 'totmem', 'size']:
                if not prop in status:
                    continue
                val = status.get(prop)[0]
                if prop in ('size',):
                    # 'size': ['539214180kb:539416640kb']
                    # - use 2nd field
                    val = val.split(':')[1]
                derived[prop] = str2byte(val)

        full_state['derived'] = derived

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
        derived = full_state['derived']

        # what state to report?
        state_list.append(derived['state'])

        if derived['nodestate'] == NDST_OK:
            cores = derived.get('np', None)
            physmem = derived.get('physmem', None)
            totmem = derived.get('totmem', None)
            size = derived.get('size', None)

            if not all(cores, physmem, totmem, size):  # there shouldn't be any value 0
                # round mem to 1 gb, size to 5gb
                GB = str2byte('gb')
                pmem = ceil(10 * physmem / GB) / 10
                tmem = ceil(10 * totmem / GB) / 10
                swap = tmem - pmem
                dsize = ceil(10 * size / (5 * GB)) / 2
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


