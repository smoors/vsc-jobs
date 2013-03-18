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
from vsc import fancylogger
from vsc.utils.missing import all
from vsc.jobs.pbs.interface import get_query
from vsc.jobs.pbs.tools import str2byte, str2sec

_log = fancylogger.getLogger('pbs.jobs', fname=False)


JOBID_REG = re.compile(r"\w+/\w+(\.|\w|\[|\])+")


def get_jobs():
    """Get the jobs"""
    query = get_query()
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
