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
@author: Ward Poelmans (Vrije Universiteit Brussel)
@author: Samuel Moors (Vrije Universiteit Brussel)
"""

import os
import logging
import pwd
import sys

from vsc.jobs.pbs.clusterdata import get_clusterdata, get_cluster_mpp, get_cluster_overhead
from vsc.jobs.pbs.clusterdata import MASTER_REGEXP, GPUFEATURES, CPUFEATURES, EXCLUSIVEFEATURES, ALLFEATURES
from vsc.jobs.pbs.clusterdata import get_cluster_maxppn, get_cluster_maxgpus
from vsc.jobs.pbs.submitfilter import SubmitFilter, get_warnings, warn, PMEM, VMEM, abort
from vsc.jobs.pbs.submitfilter import MEM, _parse_mem_units, FEATURE
from vsc.utils import fancylogger

fancylogger.setroot()
fancylogger.logToScreen(enable=False)
fancylogger.logToDevLog(True)
fancylogger.setLogLevelInfo()

ENV_NODE_PARTITION = 'VSC_NODE_PARTITION'
ENV_RESERVATION = 'VSC_RESERVATION'

MIN_MEM = MIN_VMEM = 1 << 30  # minimum allowed requested memory


def make_new_header(sf):
    """
    Generate a new header by rewriting selected options and adding missing ones.

    Takes a submitfilter instance as only argument,
    returns the header as a list of strings (one line per element)
    """
    state, newopts = sf.gather_state(MASTER_REGEXP)

    cluster = state['_cluster']
    ppn = state['l'].get('_ppn', 1)
    gpus = state['l'].get('_nrgpus', 1)

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

    # check if requested features are valid
    feature_list = filter(None, state['l'].get(FEATURE, '').split(':'))
    req_features = set(feature_list + state['l']['_features'])
    invalid_features = [x for x in req_features if x not in ALLFEATURES]
    if invalid_features:
        abort('feature(s) not valid (%s).' % ', '.join(invalid_features))

    cpufeat = req_features.intersection(CPUFEATURES)
    if len(cpufeat) > 1:
        abort('more than one CPU architecture requested (%s).' % ', '.join(cpufeat))

    gpufeat = req_features.intersection(GPUFEATURES)

    # add feature 'gpgpu' and queue 'gpu'
    # set cluster to 'gpgpu'
    if gpus > 0:
        cluster = 'gpgpu'
        if not gpufeat:
            feature = ':'.join(feature_list + ['gpgpu'])
            state['l'].update({
                FEATURE: "%s" % feature,
            })
            header.extend([
                "# Add feature gpgpu - added by submitfilter",
                make("-l", "%s=%s" % (FEATURE, feature)),
            ])
        header.extend([
            "# Submit to gpu queue - added by submitfilter",
            make("-q", "gpu")
        ])

    # make sure that gpu nodes are only requested when gpus > 0
    if gpus == 0 and gpufeat:
        abort('requested gpus (%s) should be at least 1 when requesting feature (%s).' % (gpus, ', '.join(gpufeat)))

    # check for mutually exclusive features
    excl_feat = [x for x req_features if x in EXCLUSIVEFEATURES]
    if len(excl_feat) > 1:
        abort('requested combination of features is not available (%s).' % ', '.join(excl_feat))

    # select cluster corresponding to specific features:
    if excl_feat:
        cluster = excl_feat[0]

    warn("Using server: %s" % cluster)

    # vmem: add default when not specified
    if VMEM not in state['l'] and PMEM not in state['l'] and MEM not in state['l']:
        (_, vpp) = get_cluster_mpp(cluster)
        vmem = vpp * ppn
        state['l'].update({
            VMEM: "%s" % vmem,
            '_%s' % VMEM: vmem,
        })
        header.extend([
            "# No pmem or vmem limit specified - added by submitfilter",
            make("-l", "%s=%s" % (VMEM, vmem)),
            make("-l", "%s=%s" % (MEM, vmem)),
        ])
        logging.warn("submitfilter - no [vp]mem specified by user %s. adding %s", current_user, vmem)
    else:
        try:
            requested_memory = (VMEM, state['l'][VMEM])
            # force virtual memory to be at least equal to MIN_VMEM
            if _parse_mem_units(requested_memory[1]) < MIN_VMEM:
                vmem = MIN_VMEM
                state['l'].update({
                    VMEM: "%s" % vmem,
                    '_%s' % VMEM: vmem,
                })
                header.extend([
                    "# Force vmem limit equal to MIN_VMEM - added by submitfilter",
                    make("-l", "%s=%s" % (VMEM, vmem)),
                ])
            # force/add mem equal to vmem
            header.extend([
                "# Force mem limit equal to vmem - added by submitfilter",
                make("-l", "%s=%s" % (MEM, vmem)),
            ])
        except KeyError:
            try:
                requested_memory = (PMEM, state['l'][PMEM])
            except KeyError:
                requested_memory = (MEM, state['l'][MEM])
                # force memory to be at least equal to MIN_MEM
                mem = _parse_mem_units(requested_memory[1])
                if mem < MIN_MEM:
                    mem = MIN_MEM
                    state['l'].update({
                        MEM: "%s" % mem,
                        '_%s' % MEM: mem,
                    })
                    header.extend([
                        "# Force mem limit equal to MIN_MEM - added by submitfilter",
                        make("-l", "%s=%s" % (MEM, mem)),
                    ])
                # force/add vmem equal to mem
                header.extend([
                    "# Force vmem limit equal to mem - added by submitfilter",
                    make("-l", "%s=%s" % (VMEM, mem)),
                ])

        logging.info("submitfilter - %s requested by user %s was %s",
                     requested_memory[0], current_user, requested_memory[1])

    # this is only for testing, should be removed in prod
    warn('state_l: %s' % state['l'])

    # check that gpus is not more than available
    maxgpus = get_cluster_maxgpus(cluster)
    if gpus > maxgpus:
        abort('requested gpus (%s) is more than the maximum available (%s).' % (gpus, maxgpus))

    # check that ppn is not more than available
    maxppn = get_cluster_maxppn(cluster)
    if ppn > maxppn:
        abort('requested ppn (%s) is more than the maximum available (%s).' % (ppn, maxppn))

    # test/warn:
    cl_data = get_clusterdata(cluster)

    #    cores on cluster: warn when non-ideal number of cores is used (eg 8 cores on 6-core numa domain etc)
    #    ideal: either less than NP_LCD or multiple of NP_LCD
    np_lcd = cl_data['NP_LCD']

    if ppn > np_lcd and ppn % np_lcd:
        warn('The chosen ppn (%s) is not considered ideal: should use either lower than %s or a multiple of %s' %
             (ppn, np_lcd, np_lcd))

    # vmem, mem, pmem too high: job will not start
    overhead = get_cluster_overhead(cluster)
    availmem = cl_data['TOTMEM'] - overhead
    physmem = cl_data['PHYSMEM'] - overhead
    if state['l'].get('_%s' % VMEM) > availmem:
        requested = state['l'].get('_%s' % VMEM) or state['l'].get('_%s' % MEM)
        abort("requested %sb vmem per node, this is more than the available vmem (%sb)." % (requested, availmem))
    elif state['l'].get('_%s' % MEM) > physmem:
        requested = state['l'].get('_%s' % MEM)
        abort("requested %sb mem per node, this is more than the available mem (%sb)." % (requested, physmem))
    elif state['l'].get('_%s' % PMEM) > physmem / maxppn:
        requested = state['l'].get('_%s' % PMEM)
        abort("requested %sb pmem per node, this is more than the available pmem (%sb)." %
              (requested, physmem / maxppn))

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

    header = make_new_header(sf)

    # this is only for testing, should be removed in prod
    sys.stderr.write("\n".join(arguments) + "\n")
    sys.stderr.write("\n".join(header + [sf.prebody]) + "\n")

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
