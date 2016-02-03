#!/usr/bin/env python
#
# Copyright 2013-2016 Ghent University
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
Filters scripts submitted by qsub,
adds default values and command line parameters to the script
for processing by pbs

@author: Jens Timmerman (Ghent University)
@author: Stijn De Weirdt (Ghent University)
"""

import sys
import re
import os

from vsc.jobs.pbs.clusterdata import get_clusterdata, get_cluster_mpp, get_cluster_overhead, MASTER_REGEXP
from vsc.jobs.pbs.submitfilter import SubmitFilter, get_warnings, warn


def make_new_header(sf):
    """
    Generate a new header by rewriting selected options and add missing ones.

    Takes a submitfilter instance as only argument,
    returns the header as a list of strings (one line per element)
    """

    # very VSC specific (or only ugent?)
    master_reg = re.compile(r'master[^.]*\.([^.]+)\.(?:[^.]+\.vsc|os)$')

    state, newopts = sf.gather_state(MASTER_REGEXP)

    ppn = state['l'].get('_ppn', 1)
    make = sf.make_header

    # make a copy, leave original untouched
    header = sf.header[:]

    # resources: rewrite all resource lines
    for (opt, orig), idx, new in zip(sf.allopts, sf.occur, newopts):
        if opt == 'l' and idx is not None:
            header[idx] = header[idx].replace(orig, new)

    # fix missing
    #
    #    mail: force no mail when no mail is specified
    if 'm' not in state:
        header.extend([
            "# No mail specified - added by submitfilter",
            make("-m","n"),
        ])

    #    vmem: add default when not specified
    if not 'vmem' in state['l']:
        (ppp, vpp) = get_cluster_mpp(state['_cluster'])
        vmem = vpp * ppn
        state['l'].update({
            'vmem': "%s" % vmem,
            '_vmem': vmem,
        })
        header.extend([
            "# No vmem limit specified - added by submitfilter (server found: %s)" % state['_cluster'],
            make("-l", "vmem=%s" % vmem),
        ])

    #    check whether VSC_NODE_PARTITION environment variable is set
    if 'VSC_NODE_PARTITION' in os.environ:
        header.extend([
            "# Adding PARTITION as specified in VSC_NODE_PARTITION",
            make("-W", "x=PARTITION:%s" % os.environ['VSC_NODE_PARTITION']),
        ])

    # test/warn:
    cl_data = get_clusterdata(state['_cluster'])

    #    cores on cluster: warn when non-ideal number of cores is used (eg 8 cores on 6-core numa domain etc)
    #    ideal: either less than NP_LCD or multiple of NP_LCD
    np_lcd = cl_data['NP_LCD']

    if ppn > np_lcd and ppn % np_lcd:
        warn('The chosen ppn %s is not considered ideal: should use either lower than or multiple of %s' %
             (ppn, np_lcd))

    #    vmem too high: job will not start
    overhead = get_cluster_overhead(state['_cluster'])
    availmem = cl_data['TOTMEM'] - overhead
    if state['l'].get('_vmem') > availmem:
        warn("Warning, requested %sb vmem per node, this is more than the available vmem (%sb), this"
             " job will never start." % (state['l']['_vmem'], availmem))

    #    TODO: mem too low on big-memory systems ?

    return header


def main(arguments=None):
    """Main function"""

    if arguments is None:
        arguments = sys.argv

    sf = SubmitFilter(arguments, sys.stdin.readline)
    sf.parse_header()

    header = make_new_header(sf)

    # flush it so it doesn't get mixed with stderr
    sys.stdout.flush()
    sys.stderr.flush()

    # prebody is not stripped of the newline
    sys.stdout.write("\n".join(header+[sf.prebody]))
    for line in sf.stdin:
        sys.stdout.write(line)

    # print all generated warnings
    # flush it so it doesn't get mixed with stderr
    sys.stdout.flush()
    for warn in ["%s\n" % w for w in get_warnings()]:
        sys.stderr.write(warn)
    sys.stderr.flush()

    sys.exit(0)

if __name__ == '__main__':
    main()
