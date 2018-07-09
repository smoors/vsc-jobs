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
Filters scripts submitted by qsub,
adds default values and command line parameters to the script
for processing by pbs

@author: Jens Timmerman (Ghent University)
@author: Stijn De Weirdt (Ghent University)
"""

import os
import logging
import pwd
import sys

from vsc.jobs.pbs.clusterdata import get_clusterdata, get_cluster_mpp, get_cluster_overhead, MASTER_REGEXP
from vsc.jobs.pbs.clusterdata import DEFAULT_SERVER_CLUSTER, GPUFEATURES, CPUFEATURES, FEATURES
from vsc.jobs.pbs.submitfilter import SubmitFilter, get_warnings, warn, PMEM, VMEM
from vsc.jobs.pbs.submitfilter import MEM
from vsc.utils import fancylogger

fancylogger.setroot()
fancylogger.logToScreen(enable=False)
fancylogger.logToDevLog(True)
fancylogger.setLogLevelInfo()

ENV_NODE_PARTITION = 'VSC_NODE_PARTITION'
ENV_RESERVATION = 'VSC_RESERVATION'


def make_new_header_vub(sf):
    """
    Generate a new header by rewriting selected options and adding missing ones.

    Takes a submitfilter instance as only argument,
    returns the header as a list of strings (one line per element)
    """
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
            make("-m", "n"),
        ])

    current_user = pwd.getpwuid(os.getuid()).pw_name

    # vmem: add default when not specified
    if VMEM not in state['l'] and PMEM not in state['l'] and MEM not in state['l']:
        (_, vpp) = get_cluster_mpp(state['_cluster'])
        vmem = vpp * ppn
        state['l'].update({
            VMEM: "%s" % vmem,
            '_%s' % VMEM: vmem,
        })
        header.extend([
            "# No pmem or vmem limit specified - added by submitfilter (server found: %s)" % state['_cluster'],
            make("-l", "%s=%s" % (VMEM, vmem)),
        ])
        logging.warn("submitfilter - no [vp]mem specified by user %s. adding %s", current_user, vmem)
    else:
        try:
            requested_memory = (VMEM, state['l'][VMEM])
        except KeyError:
            try:
                requested_memory = (PMEM, state['l'][PMEM])
            except KeyError:
                requested_memory = (MEM, state['l'][MEM])
                # add vmem equal to mem
                header.extend([
                    "# Force vmem limit equal to mem - added by submitfilter (server found: %s)" % state['_cluster'],
                    make("-l", "%s=%s" % (VMEM, requested_memory)),
                ])

        logging.info("submitfilter - %s requested by user %s was %s",
                     requested_memory[0], current_user, requested_memory[1])

    # test/warn:
    cl_data = get_clusterdata(state['_cluster'])

    #    cores on cluster: warn when non-ideal number of cores is used (eg 8 cores on 6-core numa domain etc)
    #    ideal: either less than NP_LCD or multiple of NP_LCD
    np_lcd = cl_data['NP_LCD']

    if ppn > np_lcd and ppn % np_lcd:
        warn('The chosen ppn %s is not considered ideal: should use either lower than or multiple of %s' %
             (ppn, np_lcd))

    if state['l'].get('_nrgpus') > 0:
        make("-l", "feature=gpgpu")
        make("-q", "gpu")

    if state['_cluster'] != DEFAULT_SERVER_CLUSTER:
        # vmem, mem, pmem too high: job will not start
        overhead = get_cluster_overhead(state['_cluster'])
        availmem = cl_data['TOTMEM'] - overhead
        physmem = cl_data['PHYSMEM'] - overhead
        if state['l'].get('_%s' % VMEM) > availmem:
            requested = state['l'].get('_%s' % VMEM) or state['l'].get('_%s' % MEM)
            warn("Warning, requested %sb vmem per node, this is more than the available vmem (%sb), this"
                 " job will never start." % (requested, availmem))
        elif state['l'].get('_%s' % MEM) > physmem:
            requested = state['l'].get('_%s' % MEM)
            warn("Warning, requested %sb mem per node, this is more than the available mem (%sb), this"
                 " job will never start." % (requested, physmem))
        elif state['l'].get('_%s' % PMEM) > physmem / cl_data['NP']:
            requested = state['l'].get('_%s' % PMEM)
            warn("Warning, requested %sb pmem per node, this is more than the available pmem (%sb), this"
                 " job will never start." % (requested, physmem / cl_data['NP']))

    return header


def make_new_header(sf):
    """
    Generate a new header by rewriting selected options and adding missing ones.

    Takes a submitfilter instance as only argument,
    returns the header as a list of strings (one line per element)
    """
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
            make("-m", "n"),
        ])

    current_user = pwd.getpwuid(os.getuid()).pw_name

    # vmem: add default when not specified
    if VMEM not in state['l'] and PMEM not in state['l'] and MEM not in state['l']:
        (_, vpp) = get_cluster_mpp(state['_cluster'])
        vmem = vpp * ppn
        state['l'].update({
            VMEM: "%s" % vmem,
            '_%s' % VMEM: vmem,
        })
        header.extend([
            "# No pmem or vmem limit specified - added by submitfilter (server found: %s)" % state['_cluster'],
            make("-l", "%s=%s" % (VMEM, vmem)),
        ])
        logging.warn("submitfilter - no [vp]mem specified by user %s. adding %s", current_user, vmem)
    else:
        try:
            requested_memory = (VMEM, state['l'][VMEM])
        except KeyError:
            try:
                requested_memory = (PMEM, state['l'][PMEM])
            except KeyError:
                requested_memory = (MEM, state['l'][MEM])

        logging.info("submitfilter - %s requested by user %s was %s",
                     requested_memory[0], current_user, requested_memory[1])

    #  check whether VSC_NODE_PARTITION environment variable is set
    if ENV_NODE_PARTITION in os.environ:
        header.extend([
            "# Adding PARTITION as specified in %s" % ENV_NODE_PARTITION,
            make("-W", "x=PARTITION:%s" % os.environ[ENV_NODE_PARTITION]),
        ])

    #  check whether VSC_RESERVATION environment variable is set
    if ENV_RESERVATION in os.environ:
        header.extend([
            "# Adding reservation as specified in %s" % ENV_RESERVATION,
            make("-W", "x=FLAGS:ADVRES:%s" % os.environ[ENV_RESERVATION]),
        ])

    # test/warn:
    cl_data = get_clusterdata(state['_cluster'])

    #    cores on cluster: warn when non-ideal number of cores is used (eg 8 cores on 6-core numa domain etc)
    #    ideal: either less than NP_LCD or multiple of NP_LCD
    np_lcd = cl_data['NP_LCD']

    if ppn > np_lcd and ppn % np_lcd:
        warn('The chosen ppn %s is not considered ideal: should use either lower than or multiple of %s' %
             (ppn, np_lcd))

    # vmem, mem, pmem too high: job will not start
    overhead = get_cluster_overhead(state['_cluster'])
    availmem = cl_data['TOTMEM'] - overhead
    physmem = cl_data['PHYSMEM'] - overhead
    if state['l'].get('_%s' % VMEM) > availmem:
        requested = state['l'].get('_%s' % VMEM) or state['l'].get('_%s' % MEM)
        warn("Warning, requested %sb vmem per node, this is more than the available vmem (%sb), this"
             " job will never start." % (requested, availmem))
    elif state['l'].get('_%s' % MEM) > physmem:
        requested = state['l'].get('_%s' % MEM)
        warn("Warning, requested %sb mem per node, this is more than the available mem (%sb), this"
             " job will never start." % (requested, physmem))
    elif state['l'].get('_%s' % PMEM) > physmem / cl_data['NP']:
        requested = state['l'].get('_%s' % PMEM)
        warn("Warning, requested %sb pmem per node, this is more than the available pmem (%sb), this"
             " job will never start." % (requested, physmem / cl_data['NP']))

    return header


def main(arguments=None):
    """Main function"""

    if arguments is None:
        arguments = sys.argv

    # This error could otherwise result in empty PBS_O_WORKDIR
    try:
        os.getcwd()
    except OSError as e:
        sys.stderr.write("ERROR: Unable to determine current workdir: %s (PWD deleted?)." % e)
        sys.stderr.flush()
        sys.exit(1)

    sf = SubmitFilter(arguments, sys.stdin.readline)
    sf.parse_header()

    header = make_new_header_vub(sf)

    # flush it so it doesn't get mixed with stderr
    sys.stdout.flush()
    sys.stderr.flush()

    # prebody is not stripped of the newline
    sys.stdout.write("\n".join(header + [sf.prebody]))
    for line in sf.stdin:
        sys.stdout.write(line)

    # print all generated warnings
    # flush it so it doesn't get mixed with stderr
    sys.stdout.flush()
    for warning in ["%s\n" % w for w in get_warnings()]:
        sys.stderr.write(warning)
    sys.stderr.flush()

    sys.exit(0)


if __name__ == '__main__':
    main()
