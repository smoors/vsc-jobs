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
                   ND_down_on_error: 'x',
                   ND_bad: 'b',
}

ND_STATE_EXOTIC = [
                   ND_busy,
                   ND_reserve,
                   ND_timeshared,
                   ND_job_sharing,
                   ND_state_unknown,
                   ND_cluster,
                   ND_bad,
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

JOBID_REG = re.compile(r"\w+/\w+(\.|\w|\[|\])+")


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


def str2sec(txt):
    """Convert a DD:HH:MM:SS format to seconds"""
    reg_time = re.compile(r"((((?P<day>\d+):)?(?P<hour>\d+):)?(?P<min>\d+):)?(?P<sec>\d+)")
    m = reg_time.search(txt)
    if m:
        totalwallsec = int(m.group('sec'))
        if m.group('min'):
            totalwallsec += int(m.group('min')) * 60
            if m.group('hour'):
                totalwallsec += int(m.group('hour')) * 60 * 60
                if m.group('day'):
                    totalwallsec += int(m.group('day')) * 60 * 60 * 24
        return totalwallsec
    else:
        return None


def get_nodes_dict():
    """Get the pbs_nodes equivalent info as dict"""
    query = PBSQuery()
    node_states = query.getnodes([])
    for full_state in node_states.values():
        state = full_state['state'][0]  # insert additional state
        if 'jobs' in full_state and not all([JOBID_REG.search(x.strip()) for x in full_state['jobs']]):
            full_state['state'].insert(0, ND_bad)
        elif state == ND_free and 'jobs' in full_state:
            full_state['state'].insert(0, ND_free_and_job)
        elif state == pbs.ND_down and 'error' in full_state:
            full_state['state'].insert(0, ND_down_on_error)

        # extend the node dict with derived dict (for convenience)
        derived = {}

        derived['state'] = state

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
        state = full_state['state'][0]
        state_list.append(state)

        if not state in ND_STATE_NOTOK:
            derived = full_state['derived']
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


def get_jobs():
    """Get the jobs"""
    query = PBSQuery()
    jobs = query.getjobs()
    return jobs


def get_jobs_dict():
    """Get jobs dict with derived info"""
    jobs = get_jobs()

    reg_user = re.compile(r"(?P<user>\w+)@\S+")

    nodes_cores = re.compile(r"(?P<nodes>\d+)(:ppn=(?P<cores>\d+))?")
    nodes_nocores = re.compile(r"(?P<nodes>node\d+).*?")

    for jobdata in jobs.values():
        derived = {}

        derived['state'] = jobdata['job_state'][0]

        r = reg_user.search(jobdata['Job_Owner'][0])
        if r:
            derived['user'] = r.group('user')

        if 'Resource_List' in jobdata:
            resource_list = jobdata['Resource_List']

            # walltime
            if 'walltime' in resource_list:
                totalwallsec = str2sec(resource_list['walltime'][0])
                if totalwallsec is not None:
                    derived['totalwalltimesec'] = totalwallsec

            # nodes / cores
            if 'neednodes' in resource_list:
                m = nodes_cores.match(resource_list['neednodes'][0])
                if not m:
                    if nodes_nocores.match(resource_list['neednodes'][0]):
                        m = nodes_cores.match("1")
            elif 'nodes' in resource_list:
                m = nodes_cores.match(resource_list['nodes'][0])
            if m:
                nodes = int(m.group('nodes'))
                cores = 1
                if len(m.groups()) > 1 and m.group('cores'):
                    cores = int(m.group('cores'))
                derived['nodes'] = nodes
                derived['cores'] = cores

        # resource used
        if 'resources_used' in jobdata:
            resources_used = jobdata['resources_used']

            if 'mem' in resources_used:
                derived['used_mem'] = str2byte(resource_list['mem'][0])

            if 'vmem' in resources_used:
                derived['used_vmem'] = str2byte(resource_list['vmem'][0])

            if 'walltime' in resources_used:
                sec = str2sec(resource_list['walltime'][0])
                if sec is not None:
                    derived['used_walltime'] = sec

            if 'cput' in resources_used:
                sec = str2sec(resource_list['cput'][0])
                if sec is not None:
                    derived['used_cput'] = sec

        if 'exec_host' in jobdata:
            exec_hosts = {}
            for host in jobdata['exec_host'][0].split('+'):
                hostname = host.split('/')[0]
                if not hostname in exec_hosts:
                    exec_hosts[hostname] = 0
                exec_hosts[hostname] += 1
            derived['exec_hosts'] = exec_hosts

        jobdata['derived'] = derived

    return jobs


def get_userjob_stats():
    """Report job stats per user"""
    jobs = get_jobs_dict()

    faults = []
    stats = {}

    # order as printed by nagios
    categories = [
                  ('R', 'running'),
                  ('RN', 'running nodes'),
                  ('RC', 'running cores'),
                  ('RP', 'running procseconds'),

                  ('Q', 'queued'),
                  ('QN', 'queued nodes'),
                  ('QC', 'queued cores'),
                  ('QP', 'queued procseconds'),

                  # this one last
                  ('O', 'other jobids')
                  ]

    cat_map = dict([(x[0], idx) for idx, x in enumerate(categories)])

    for name, jobdata in jobs.items():
        derived = jobdata['derived']

        if not 'user' in derived:
            faults.append(('Missing user in job %s' % name, jobdata))
            continue

        if not derived['user'] in stats:
            stats[derived['user']] = [0] * (len(categories) - 1) + [[]]
        ustat = stats[derived['user']]

        if 'totalwalltimesec' in derived:
            totalwalltimesec = derived['totalwalltimesec']
        else:
            faults.append(('Missing totalwalltimesec in job %s. Counts as 0.' % (name), jobdata))
            totalwalltimesec = 0

        if not 'nodes' in derived:
            faults.append(('Missing nodes/cores in job %s. Marked as other.' % (name), jobdata))
            ustat[-1].append(name)
            continue

        nodes = derived['nodes']
        cores = derived['cores']
        corenodes = nodes * cores

        state = derived['state']
        if state in ('R', 'Q',):
            pass
        elif state in ('H',):
            state = 'Q'
        else:
            faults.append(('Not counting job with state %s in job %s. Marked as other.' % (name, state), jobdata))
            ustat[-1].append(name)
            continue

        ustat[cat_map['%s' % state]] += 1
        ustat[cat_map['%sN' % state]] += nodes
        ustat[cat_map['%sC' % state]] += cores
        ustat[cat_map['%sP' % state]] += corenodes * totalwalltimesec

    return stats, faults, categories
