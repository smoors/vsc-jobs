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
The main pbs module

@author: Stijn De Weirdt (Ghent University)
"""
import re
from vsc.jobs.pbs.interface import get_query
from vsc.utils import fancylogger

_log = fancylogger.getLogger('pbs.queues', fname=False)


def get_queues():
    """Get the queues"""
    query = get_query()
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
        if not queue.get('enabled', None):
            queues_dict['disabled'].append((name, queue))
        elif queue['queue_type'][0].lower() == 'route':
            queues_dict['route'].append((name, queue))
        else:
            queues_dict['enabled'].append((name, queue))

    return queues_dict


