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
show_nodes prints nodes and node state information

@author: Stijn De Weirdt (Ghent University)
"""

import sys
from vsc.utils import fancylogger
from vsc.jobs.pbs.nodes import get_nodes, collect_nodeinfo, NDNAG_CRITICAL, NDNAG_WARNING, NDNAG_OK
from vsc.jobs.pbs.nodes import ND_NAGIOS_CRITICAL, ND_NAGIOS_WARNING, ND_NAGIOS_OK, ND_down, ND_offline
from vsc.jobs.pbs.nodes import ND_free, ND_free_and_job, ND_job_exclusive
from vsc.jobs.pbs.nodes import ND_state_unknown, ND_bad, ND_error, ND_idle, ND_down_on_error
from vsc.jobs.pbs.moab import get_nodes_dict as moab_get_nodes_dict
from vsc.utils.generaloption import simple_option
from vsc.utils.missing import any
from vsc.utils.nagios import NagiosResult, warning_exit, ok_exit, critical_exit, unknown_exit

_log = fancylogger.getLogger('show_nodes')


def main():
    """Main"""

    options = {
        'nagios':('Report in nagios format', None, 'store_true', False, 'n'),
        'regex':('Filter on regexp, data for first match', None, 'regex', None, 'r'),
        'allregex':('Combined with --regex/-r, return all data', None, 'store_true', False, 'A'),
        'anystate':('Matches any state (eg down_on_error node will also list as error)',
                    None, 'store_true', False, 'a') ,
        'down':('Down nodes', None, 'store_true', False, 'D'),
        'downonerror':('Down on error nodes', None, 'store_true', False, 'E'),
        'offline':('Offline nodes', None, 'store_true', False, 'o'),
        'partial':('Partial nodes (one or more running job(s), jobslot(s) available)', None, 'store_true', False, 'p'),
        'job-exclusive':('Job-exclusive nodes (no jobslots available)', None, 'store_true', False, 'x'),
        'free':('Free nodes (0 or more running jobs, jobslot(s) available)', None, 'store_true', False, 'f'),
        'unknown':('State unknown nodes', None, 'store_true', False, 'u'),
        'bad':('Bad nodes (broken jobregex)', None, 'store_true', False, 'b'),
        'error':('Error nodes', None, 'store_true', False, 'e'),
        'idle':('Idle nodes (No running jobs, jobslot(s) available)', None, 'store_true', False, 'i'),
        'singlenodeinfo':(('Single (most-frequent) node information in key=value format'
                           '(no combination with other options)'), None, 'store_true', False, 'I'),
        'reportnodeinfo':('Report node information (no combination with other options)',
                          None, 'store_true', False, 'R'),
        'moab':('Use moab information (mdiag -n)', None, 'store_true', False, 'm'),
        'moabxml':('Use xml moab data from file (for testing)', None, 'store', None),
        'shorthost':('Return (short) hostname', None, 'store_true', False, 's'),
        'invert':('Return inverted selection', None, 'store_true', False, 'v'),
        }

    go = simple_option(options)

    all_states = ND_NAGIOS_CRITICAL + ND_NAGIOS_WARNING + ND_NAGIOS_OK
    report_states = []
    if go.options.down:
        report_states.append(ND_down)
    if go.options.downonerror:
        report_states.append(ND_down_on_error)
    if go.options.offline:
        report_states.append(ND_offline)
    if go.options.free:
        report_states.append(ND_free)
    if go.options.partial:
        report_states.append(ND_free_and_job)
    if go.options.job_exclusive:
        report_states.append(ND_job_exclusive)
    if go.options.unknown:
        report_states.append(ND_state_unknown)
    if go.options.bad:
        report_states.append(ND_bad)
    if go.options.error:
        report_states.append(ND_error)
    if go.options.idle:
        report_states.append(ND_idle)

    if len(report_states) == 0:
        report_states = all_states

    if go.options.singlenodeinfo or go.options.reportnodeinfo:
        nodeinfo = collect_nodeinfo()[2]
        if len(nodeinfo) == 0:
            _log.error('No nodeinfo found')
            sys.exit(1)

        ordered = sorted(nodeinfo.items(), key=lambda x: len(x[1]), reverse=True)

        if go.options.singlenodeinfo:
            if len(nodeinfo) > 1:
                msg = "Not all nodes have same parameters. Using most frequent ones."
                if go.options.reportnodeinfo:
                    _log.warning(msg)
                else:
                    _log.error(msg)

            # usage: export `./show_nodes -I` ; env |grep SHOWNODES_
            most_freq = ordered[0][0]
            msg = []
            msg.append("SHOWNODES_PPN=%d" % most_freq[0])
            msg.append("SHOWNODES_PHYSMEMMB=%d" % (most_freq[1] * 1024))
        else:
            msg = []
            for info, nodes in ordered:
                txt = "%d nodes with %d cores, %s MB physmem, %s GB swap and %s GB local disk" % (
                        len(nodes), info[0], info[1] * 1024, info[2], info[3])
                msg.append(txt)
                # print and _log are dumped to stdout at different moment, repeat the txt in the debug log
                _log.debug("Found %s with matching nodes: %s" % (txt, nodes))

        print "\n".join(msg)
        sys.exit(0)

    if go.options.moab:

        if go.options.moabxml:
            try:
                moabxml = open(go.options.moabxml).read()
            except:
                _log.error('Failed to read moab xml from %s' % go.options.moabxml)
        else:
            moabxml = None
        nodes_dict = moab_get_nodes_dict(xml=moabxml)

        nodes = get_nodes(nodes_dict)
    else:
        nodes = get_nodes()


    # WARNING first, since that is the one that gives dependency on others
    nagiosstatesorder = [NDNAG_WARNING, NDNAG_CRITICAL, NDNAG_OK]
    nagiosexit = {
                  NDNAG_CRITICAL: critical_exit,
                  NDNAG_WARNING: warning_exit,
                  NDNAG_OK: ok_exit,
                  }

    nagios_res = {}
    detailed_res = {}
    nodes_found = []

    all_nodes = []

    for name, full_state in nodes:
        all_nodes.append(name)

        if go.options.regex and not go.options.regex.search(name):
            continue

        nagios_state = full_state['derived']['nagiosstate']
        if not nagios_state in nagios_res:
            nagios_res[nagios_state] = []

        state = full_state['derived']['state']
        states = full_state['derived']['states']

        if state == ND_free and ND_idle in states:
            state = ND_idle  # special case for idle
        if not state in detailed_res:
            detailed_res[state] = []

        if go.options.anystate:
            states_to_check = states
        else:
            states_to_check = [state]

        # filter the allowed states
        if any(x for x in states_to_check if x in report_states):
            nagios_res[nagios_state].append(states)
            detailed_res[state].append(states)
            nodes_found.append(name)

            if go.options.regex and not go.options.allregex:
                break

    if go.options.invert:
        nodes_found = [x for x in all_nodes if not x in nodes_found]

    if go.options.regex and not go.options.allregex:
        # there should only be one node
        nagios_state, all_states = nagios_res.items()[0]
        states = all_states[0]
        if go.options.nagios:
            msg = "show_nodes - %s" % ",".join(states)
            nagiosexit[nagios_state](msg)
        else:
            txt = "%s %s" % (nagios_state, ",".join(states))
            print txt
    else:
        if go.options.nagios:
            msg = NagiosResult('show_nodes')
            txt = []
            total = 0
            for state in all_states:
                if state in detailed_res:
                    nr = len(detailed_res[state])
                else:
                    nr = 0
                total += nr
                setattr(msg, state, nr)
            msg.total = total

            reported_state = [str(NDNAG_OK), '']
            if ND_bad in detailed_res:
                reported_state[0] = NDNAG_CRITICAL
                msg.message += ' - %s bad nodes' % (len(detailed_res[ND_bad]))
            nagiosexit[reported_state[0]](msg)
        else:
            # just print the nodes
            if go.options.shorthost:
                nodes_found = [x.split('.')[0] for x in nodes_found]
            print ' '.join(nodes_found)


if __name__ == '__main__':
    main()

