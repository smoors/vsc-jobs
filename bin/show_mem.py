#!/usr/bin/python
#
# Copyright 2013-2015 Ghent University
#
# This file is part of vsc-jobs,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
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
This script reports jobs that are using more memory per core then the node average

@author: Stijn De Weirdt (Ghent University)
"""

from vsc.utils import fancylogger
from vsc.jobs.pbs.jobs import get_jobs_dict
from vsc.jobs.pbs.nodes import get_nodes_dict

_log = fancylogger.getLogger('show_mem')

def main():
    """Main method"""

    ns = {}
    for node, details in get_nodes_dict().items():
        derived = details['derived']

        if not 'np' in derived:
            _log.warning('np not in derived from node %s' % node)
            continue
        np = derived['np']

        if not 'physmem' in derived:
            _log.warning('physmem not in derived from node %s' % node)
            continue
        mem = derived['physmem']

        ns[node] = {
                    'np': np,
                    'mem': mem,
                    'avg': int(mem / np),
                    }

    toomuch = {}

    MB = 1024 ** 2

    for name, details in get_jobs_dict().items():
        derived = details['derived']
        if not derived['state'] in ('R',):
            continue

        if not 'user' in derived:
            _log.warning("no user in derived job name %s" % name)
            continue
        user = derived['user']

        if not 'used_mem' in derived:
            _log.warning("no used_mem in derived job name %s" % name)
            continue
        used_mem = derived['used_mem']

        if not 'exec_hosts' in derived:
            _log.warning("no exec_hosts in derived job name %s" % name)
            continue
        exec_hosts = derived['exec_hosts']

        cores = sum(exec_hosts.values())

        used_avg_mem = int(used_mem / cores)

        for host in exec_hosts.keys():
            # more then avg on node?
            if used_avg_mem > ns[host]['avg']:
                if not user in toomuch:
                    toomuch[user] = {}
                if not host in toomuch[user]:
                    toomuch[user][host] = []

                toomuch[user][host].append([name, used_avg_mem / MB, ns[host]['avg'] / MB])

    users = toomuch.keys()
    users.sort()

    txt = []
    for user in users:
        txt.append("%s:" % user)
        for host, jobs in toomuch[user].items():
            txt.append("\t%s:\t%s\n" % (host, ' '.join([str(x) for x in jobs])))

    if len(txt) > 0:
        print "\n".join(txt)
    else:
        print "All ok"


if __name__ == '__main__':
    main()
