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
Show job information
"""

import grp
import re
import sys

from vsc.utils.generaloption import simple_option
from vsc.jobs.pbs.jobs import get_userjob_stats, get_jobs_dict
from vsc.utils.nagios import NagiosResult, warning_exit, ok_exit, critical_exit, unknown_exit

SHOW_LIST = ['nodes']

go = None


def show_individual():
    """Show individual job details"""
    all_jobs = get_jobs_dict()
    jobid_ok = []
    # make valid job id list
    if go.options.jobs is not None:
        for jobdescr in go.options.jobs:
            reg = re.compile(r'' + jobdescr)
            for jobid in all_jobs.keys():
                if reg.search(jobid) and not jobid in jobid_ok:
                    jobid_ok.append(jobid)

    # filter out jobs
    for jobid in all_jobs.keys():
        if not jobid in jobid_ok:
            all_jobs.pop(jobid)

    # do something
    if go.options.show:
        if 'nodes' in go.options.show:
            # show the unqiue nodes, comma separated
            nodes = set()
            for job in all_jobs.values():
                if 'exec_hosts' in job['derived']:
                    nodes.update(job['derived']['exec_hosts'].keys())

            print "Nodes: %s" % ' '.join(nodes)


def show_summary():
    """Show summary data"""
    for group in go.options.groups:
        # get the members
        found_group = grp.getgrnam(group)
        group_members_idx = 3
        if found_group:
            go.options.users += found_group[group_members_idx]

    try:
        ustats, faults, categories = get_userjob_stats()
        if faults and not go.options.nagios:
            go.log.warning("Faults %s" % ([x[0] for x in faults]))
            go.log.debug("Faults %s" % (faults))
        cat_map = dict([(x[0], idx) for idx, x in enumerate(categories)])
    except Exception, err:
        msg = "show_jobs %s" % err
        critical_exit(msg)

    if go.options.users:
        # remove all non-listed users
        for user in ustats.keys():
            if not user in go.options.users:
                del ustats[user]

    agg_ans = [0] * (len(categories) - 1) + [[]]
    for user, tmp in ustats.items():
        for i in range(len(categories)):
            agg_ans[i] += tmp[i]

    def make_msg(ans, msgtxt, ustats=None):
        """Make a NagiosResult instance from agg_ans"""
        msg = NagiosResult(msgtxt)

        for i in ['R', 'Q', 'RN', 'RC', 'RP', 'QN', 'QC', 'QP']:
            setattr(msg, i, ans[cat_map[i]])
        msg.O = len(ans[cat_map['O']])
        msg.QP /= 3600
        msg.RP /= 3600
        if ustats is not None:
            msg.running = ans[0]
            msg.queued = ans[4]
            # users with Running jobs
            msg.RU = sum([x[cat_map['R']] > 0 for x in ustats.values()])
            # users with Queued jobs
            msg.QU = sum([x[cat_map['Q']] > 0 for x in ustats.values()])
            # unique users
            msg.UU = sum([x[cat_map['R']] + x[cat_map['Q']] > 0 for x in ustats.values()])
        return msg

    msg = make_msg(agg_ans, 'show_jobs', ustats=ustats)
    if go.options.nagios:
        ok_exit(msg)
    else:
        txt = []
        run_template = "%s%s running jobs on %s nodes (%s cores, %s prochours)"
        queued_template = "%s%s queued jobs for %s nodes (%s cores, %s prochours)"
        other_template = "%sOther jobs: %s (%s)"

        run_values = ('', msg.R, msg.RN, msg.RC, int(msg.RP),)
        txt.append(run_template % run_values)
        queued_values = ('', msg.Q, msg.QN, msg.QC, int(msg.QP),)
        txt.append(queued_template % queued_values)

        if msg.O:
            txt.append(other_template % ('', msg.O, ','.join(agg_ans[cat_map['O']])))

        if go.options.detailed:
            indent = " " * 2
            users = ustats.keys()
            for user in sorted(users):
                txt.append("%s%s" % (indent, user))
                ans = ustats[user]
                tmpmsg = make_msg(ans, 'tmp')
                run_values = (indent * 2, tmpmsg.R, tmpmsg.RN, tmpmsg.RC, int(tmpmsg.RP),)
                txt.append(run_template % run_values)
                queued_values = (indent * 2, tmpmsg.Q, tmpmsg.QN, tmpmsg.QC, int(tmpmsg.QP),)
                txt.append(queued_template % queued_values)
                if  tmpmsg.O:
                    txt.append(other_template % (indent * 2, tmpmsg.O, ','.join(ans[cat_map['O']])))

        print "\n".join(txt)
        sys.exit(0)

if __name__ == '__main__':
    options = {
           'detailed':('Report detailed information', None, 'store_true', False, 'D'),
           'groups':('Report for groups', None, "extend", [], 'g'),
           'nagios':('Report in nagios format', None, 'store_true', False, 'n'),
           'users':('Report for users', None, "extend", [], 'u'),
           'show':('Show details: %s' % ','.join(SHOW_LIST), "strlist", "store", None),
           'jobs':("Jobid(s)", "strlist", "store", None),
           }

    go = simple_option(options)

    if go.options.show:
        show_individual()
    else:
        show_summary()
