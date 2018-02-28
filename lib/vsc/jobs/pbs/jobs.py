#
# Copyright 2013-2018 Ghent University
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
from vsc.utils import fancylogger
from vsc.jobs.pbs.interface import get_query, pbs
from vsc.jobs.pbs.tools import str2byte, str2sec

_log = fancylogger.getLogger('pbs.jobs', fname=False)


JOBID_REG = re.compile(r"\w+/\w+(\.|\w|\[|\])+")

# qstat list + exechost + times
# l -> Resource_List
# exechost -> exec_host (ie no typo)
DEFAULT_ATTRS = ['name', 'owner', 'used', 'state', 'queue', 'session' ,'l',
                 'exechost'] + [x+'time' for x in ['start_', 'c', 'e', 'q', 'm']]


def get_jobs(attrs=None):
    """
    Get the jobs

    attrs is an optional list of PBS ATTR_ attribute names
        default is None, which uses a predefined set of attributes, i.e. not all
        if attrs is string 'ALL', gather all attributes
    """
    query = get_query()

    attrib_list = None

    if isinstance(attrs, basestring) and attrs == 'ALL':
        attrs = None
    elif attrs is None:
        attrs = DEFAULT_ATTRS

    if attrs is not None:
        attrib_list = filter(None, [getattr(pbs, 'ATTR_'+x, None) for x in attrs])

    jobs = query.getjobs(attrib_list=attrib_list)
    return jobs


def get_jobs_dict(attrs=None):
    """
    Get jobs dict with derived info

    attrs is passed to get_jobs
    """
    jobs = get_jobs(attrs=attrs)

    reg_user = re.compile(r"(?P<user>\w+)@\S+")

    nodes_cores = re.compile(r"(?P<nodes>\d+)(:ppn=(?P<cores>\d+))?")
    namednodes_cores = re.compile(r"(?P<nodes>node\d+[^:+]*)(:ppn=(?P<cores>\d+))?")
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
            need_nodes = None
            if 'neednodes' in resource_list:
                need_nodes = resource_list['neednodes'][0]
            elif 'nodes' in resource_list:
                need_nodes = resource_list['nodes'][0]
            if need_nodes is not None:
                m = nodes_cores.match(need_nodes)
                if not m:
                    namednode_m = namednodes_cores.match(need_nodes)
                    if namednode_m:
                        m = nodes_cores.match("1:ppn=%s" % (namednode_m.groups()[2] or "1"))
                    elif nodes_nocores.match(need_nodes):
                        m = nodes_cores.match("1")
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
                derived['used_mem'] = str2byte(resources_used['mem'][0])

            if 'vmem' in resources_used:
                derived['used_vmem'] = str2byte(resources_used['vmem'][0])

            if 'walltime' in resources_used:
                sec = str2sec(resources_used['walltime'][0])
                if sec is not None:
                    derived['used_walltime'] = sec

            if 'cput' in resources_used:
                sec = str2sec(resources_used['cput'][0])
                if sec is not None:
                    derived['used_cput'] = sec

        if 'exec_host' in jobdata:
            nodes = jobdata.get_nodes()
            exec_hosts = {}
            for host in nodes:
                hostname = host.split('/')[0]
                if hostname not in exec_hosts:
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

        if 'user' not in derived:
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

        if 'nodes' not in derived:
            faults.append(('Missing nodes/cores in job %s. Marked as other.' % (name), jobdata))
            ustat[-1].append(name)
            continue

        nodes = derived['nodes']
        cores = derived['cores']
        corenodes = nodes * cores

        if 'exec_hosts' in derived:
            used_cores = sum(derived['exec_hosts'].values())
            if not corenodes == used_cores:
                faults.append(('Mismatch requested %s /running %s cores in job %s. Marked as other.' %
                               (corenodes, used_cores, name), jobdata))
                ustat[-1].append(name)
                continue

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
