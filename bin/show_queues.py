#!/usr/bin/env python
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
Prints the enabled and route queues

@author: Stijn De Weirdt (Ghent University)
"""

from vsc.jobs.pbs.queues import get_queues_dict


def main():
    """Main function"""
    queues_dict = get_queues_dict()

    indent = " " * 4

    txt = []
    for name, queue in queues_dict['enabled']:
        txt.append(name)
        txt.append("%swalltime %s (max %s)" % (indent, queue['resources_default']['walltime'][0], queue['resources_max']['walltime'][0]))
    for name, queue in queues_dict['route']:
        txt.append(name)
        txt.append("%sroutes %s" % (indent, queue['route_destinations'][0]))

    print "\n".join(txt)

if __name__ == "__main__":
    main()
