#!/usr/bin/python

import sys
from vsc import fancylogger
from vsc.job.pbs.nodes import get_nodes, collect_nodeinfo, NDNAG_CRITICAL, NDNAG_WARNING, NDNAG_OK
from vsc.job.pbs.nodes import ND_NAGIOS_CRITICAL, ND_NAGIOS_WARNING, ND_NAGIOS_OK
from vsc.job.pbs.nodes import ND_down, ND_offline, ND_free, ND_job_exclusive, ND_status_unknown, ND_bad, ND_error, ND_idle
from vsc.job.pbs.moab import get_nodes_dict as moab_get_nodes_dict

_log = fancylogger.getLogger('show_nodes')

options = {
           'nagios':('Report in nagios format', None, 'store_true', False, 'n'),
           'regex':('Filter on regexp, data for first match', None, 'regex', None, 'r'),
           'allregex':('Combined with --regex/-r, return all data', None, 'store_true', False, 'R'),
           'down':('Down nodes', None, 'store_true', False, 'D'),
           'offline':('Offline nodes', None, 'store_true', False, 'o'),
           'free':('Free nodes', None, 'store_true', False, 'f'),
           'job-exclusive':('Job-exclusive nodes', None, 'store_true', False, 'x'),
           'unknown':('State unknown nodes', None, 'store_true', False, 'u'),
           'bad':('Bad nodes (broken jobregex)', None, 'store_true', False, 'b'),
           'error':('Error nodes', None, 'store_true', False, 'e'),
           'idle':('Idle nodes', None, 'store_true', False, 'i'),
           'singlenodeinfo':(('Single (most-frequent) node information in key=value format'
                              '(no combination with other options)'), None, 'store_true', False, 'I'),
           'reportnodeinfo':('Report node information (no combination with other options)', None, 'store_true', False, 'R'),
           'moab':('Use moab information (mdiag -n)', None, 'store_true', False, 'm'),
           }

go = simple_option(options)

all_states = ND_NAGIOS_CRITICAL + ND_NAGIOS_WARNING + ND_NAGIOS_OK
report_states = []
if go.options.down:
    report_states.append(ND_down)
if go.options.offline:
    report_states.append(ND_offline)
if go.options.free:
    report_states.append(ND_free)
if go.options.job_exclusive:
    report_states.append(ND_job_exclusive)
if go.options.unknown:
    report_states.append(ND_status_unknown)
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

    ordered = sorted(nodeinfo.items(), key=lambda x: x[1], reverse=True)

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
        msg.append("SHOWNODES_PHYSMEMMB=%d" % most_freq[1] / 1024 ** 2)
    else:
        msg = []
        for info, freq in ordered:
            msg.append("%d nodes with %d cores and %s MB physmem" % (freq, info[0], info[1] / 1024 ** 2))

    print "\n".join(msg)
    sys.exit(0)

if go.options.moab:
    nodes_dict = moab_get_nodes_dict()
    nodes = get_nodes(nodes_dict)
else:
    nodes = get_nodes()


# WARNING first, since that is the one that gives dependency on others
nagiosstatesorder = [NDNAG_WARNING, NDNAG_CRITICAL, NDNAG_OK]
nagiosexit = {
              NDNAG_CRITICAL:2,
              NDNAG_WARNING:1,
              NDNAG_OK: 0,
              }

nagios_res = {}
detailed_res = {}
nodes_found = []
for name, full_state in nodes:
    if go.options.regex and not go.options.regex.search(name):
        continue

    nagios_state = full_state['derived']['nagiosstate']
    if not nagios_state in nagios_res:
        nagios_res[nagios_state] = []

    state = full_state['derived']['state']
    if state == ND_free and ND_idle in full_state['state']:
        state = ND_idle  # special case for idle
    if not state in detailed_res:
        detailed_res[state] = []

    if state in report_states:  # filter the allowed states
        nagios_res[nagios_state].append(full_state['derived']['states'])
        detailed_res[state].append(full_state['derived']['states'])
        nodes_found.append(name)

        if go.options.regex and not go.options.allregex:
            break


if go.options.regex and not go.options.allregex:
    nagios_state, full_state = nagios_res.items()[0]  # there should only be one node
    if go.options.nagios:
        txt = "show_nodes %s - %s" % (nagios_state, ",".join(full_state['derived']['states']))
        print txt
        sys.exit(nagiosexit[nagios_state])
    else:
        txt = "%s %s" % (nagios_state, ",".join(full_state['derived']['states']))
        print txt
else:
    if go.options.nagios:
        header = 'show_nodes '
        txt = []
        total = 0
        for state in all_states:
            if not state in detailed_res:
                continue
            nr = len(detailed_res[state])
            total += nr
            txt.append("%s=%s" % (state, nr))
        txt.append("total=%s" % total)

        reported_state = str(NDNAG_OK)
        if ND_bad in detailed_res:
            reported_state = '%s - %s bad nodes' % (NDNAG_CRITICAL, len(detailed_res[ND_bad]))
        print "%s %s | %s" % (header, reported_state, txt)
        sys.exit(nagiosexit[reported_state])
    else:
        # just print the nodes
        print ' '.join(nodes_found)
