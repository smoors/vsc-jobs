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
The main pbs module

@author: Stijn De Weirdt (Ghent University)
"""
import re
from math import ceil
from vsc.utils import fancylogger
from vsc.jobs.pbs.interface import get_query, pbs
from vsc.jobs.pbs.tools import str2byte

_log = fancylogger.getLogger('pbs.nodes', fname=False)

# OK
ND_idle = 'idle'
ND_free_and_job = 'partial'
ND_free = pbs.ND_free
ND_job_exclusive = pbs.ND_job_exclusive
# problem
ND_offline = pbs.ND_offline
ND_offline_idle = 'idle_offline'
ND_down = pbs.ND_down
ND_down_on_error = 'down_on_error'
ND_error = 'error'
ND_bad = 'bad'
# other
ND_reserve = pbs.ND_reserve
ND_job_sharing = pbs.ND_job_sharing
ND_busy = pbs.ND_busy
ND_state_unknown = pbs.ND_state_unknown
ND_timeshared = pbs.ND_timeshared
ND_cluster = pbs.ND_cluster

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
    ND_idle: 'i',  # same as free?
    ND_offline_idle: '.',  # same as offline
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

ND_STATE_OTHER = [x for x in TRANSLATE_STATE.keys() if x not in ND_STATE_OK + ND_STATE_NOTOK]

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
    ND_idle,
]

ND_NAGIOS_WARNING = [x for x in TRANSLATE_STATE.keys() if x not in ND_NAGIOS_CRITICAL + ND_NAGIOS_OK]


JOBID_REG = re.compile(r"\w+/\w+(\.|\w|\[|\])+")

ATTR_ERROR = 'error'
ATTR_JOBS = 'jobs'
ATTR_NODESTATE = 'nodestate'
ATTR_NP = 'np'
ATTR_STATE = 'state'
ATTR_STATES = 'states'
ATTR_STATUS = 'status'


def make_state_map(derived):
    """Make a mapping for OK/NOTOK?OTHER and nagios OK/WARNING/CRITICAL.
        derived: the (reference to the) dict that is added to the node state dict as returned by pbs
            it should already contain the 'states' of the node
    """
    states = derived[ATTR_STATES]
    # what state to report?
    nd_not_ok = [x for x in ND_STATE_NOTOK if x in states]
    nd_ok = [x for x in ND_STATE_OK if x in states]
    if len(nd_not_ok) > 0:
        ndst = NDST_NOTOK
    elif (len(nd_ok)):
        ndst = NDST_OK
    else:
        ndst = NDST_OTHER
    state = states[0]
    derived[ATTR_STATE] = str(state)
    derived[ATTR_NODESTATE] = ndst

    # what nagios state?
    nag_crit = [x for x in ND_NAGIOS_CRITICAL if x in states]
    nag_warn = [x for x in ND_NAGIOS_WARNING if x in states]
    if len(nag_crit) > 0:
        ndnag = NDNAG_CRITICAL
    elif len(nag_warn) > 0:
        ndnag = NDNAG_WARNING
    else:
        ndnag = NDNAG_OK
    derived['nagiosstate'] = ndnag


def get_nodes_dict():
    """Get the pbs_nodes equivalent info as dict"""
    query = get_query()
    node_states = query.getnodes([])
    for name, full_state in node_states.items():
        # just add states
        states = full_state[ATTR_STATE]
        if ND_free in states and ATTR_JOBS in full_state:
            _log.debug('Added free_and_job node %s' % (name))
            states.insert(0, ND_free_and_job)
        if ND_free in states and ATTR_JOBS not in full_state:
            _log.debug('Append idle node %s' % (name))
            states.append(ND_idle)  # append it, not insert
        if ND_offline in states and ATTR_JOBS not in full_state:
            _log.debug('Append idle node %s' % (name))
            states.append(ND_idle)

        if ATTR_ERROR in full_state:
            _log.debug('Added error node %s' % (name))
            states.insert(0, ND_error)
        if ND_down in states and ATTR_ERROR in full_state:
            _log.debug('Added down_on_error node %s' % (name))
            states.insert(0, ND_down_on_error)

        # extend the node dict with derived dict (for convenience)
        derived = {}
        if ATTR_JOBS in full_state:
            jobs = full_state.get_jobs()
            if not all(JOBID_REG.search(x.strip()) for x in jobs):
                _log.debug('Added bad node %s for jobs %s' % (name, jobs))
                states.insert(0, ND_bad)
            derived[ATTR_JOBS] = jobs

        derived[ATTR_STATES] = [str(x) for x in states]
        make_state_map(derived)

        if ATTR_NP in full_state:
            derived[ATTR_NP] = int(full_state[ATTR_NP][0])
        if ATTR_STATUS in full_state:
            status = full_state[ATTR_STATUS]
            for prop in ['physmem', 'totmem', 'size']:
                if prop not in status:
                    continue
                val = status.get(prop)[0]
                if prop in ('size',):
                    # 'size': ['539214180kb:539416640kb']
                    # - use 2nd field
                    val = val.split(':')[1]
                derived[prop] = str2byte(val)

        full_state['derived'] = derived
        _log.debug("node %s derived data %s " % (name, derived))

    return node_states


def get_nodes(nodes_dict=None):
    """Get the pbs_nodes equivalent, return sorted list of tuples (sorted on nodename)"""
    if nodes_dict is None:
        nodes_dict = get_nodes_dict()
    node_states = nodes_dict.items()
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
        state_list.append(derived[ATTR_STATE])

        if derived[ATTR_NODESTATE] == NDST_OK:
            cores = derived.get(ATTR_NP, None)
            physmem = derived.get('physmem', None)
            totmem = derived.get('totmem', None)
            size = derived.get('size', None)

            if all([cores, physmem, totmem, size]):  # there shouldn't be any value 0
                # round mem to 1 gb, size to 5gb
                GB = str2byte('gb')
                pmem = ceil(10 * physmem / GB) / 10
                tmem = ceil(10 * totmem / GB) / 10
                swap = tmem - pmem
                dsize = ceil(10 * size / (5 * GB)) / 2
                typ = (cores, pmem, swap, dsize)
                if typ not in types:
                    types[typ] = []
                types[typ].append(node)

        result = re_host_id.search(node)
        if result:
            node_list.append(result.group('id'))
        else:
            node_list.append(str(idx + 1))  # offset +1

    return node_list, state_list, types
