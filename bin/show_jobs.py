#!/usr/bin/python
# #
# Copyright 2013-2013 Ghent University
#
# This file is part of vsc-jobs,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# http://github.com/hpcugent/vsc-jobs
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
# #
"""
Show job information
"""

import grp
import sys

from vsc.utils.generaloption import simple_option
from vsc.jobs.pbs.jobs import get_userjob_stats
from vsc.utils.nagios import NagiosResult, warning_exit, ok_exit, critical_exit, unknown_exit

options = {
           'detailed':('Report detailed information', None, 'store_true', False, 'D'),
           'groups':('Report for groups', None, "extend", [], 'g'),
           'nagios':('Report in nagios format', None, 'store_true', False, 'n'),
           'users':('Report for users', None, "extend", [], 'u'),
           }

go = simple_option(options)

for gr in go.options.groups:
    # get the members
    g = grp.getgrnam(gr)
    if g:
        go.options.users += g[3]

try:
    ustats, faults, categories = get_userjob_stats()
    if len(faults) > 0 and not go.options.nagios:
        go.log.warning("Faults %s" % ([x[0] for x in faults]))
        go.log.debug("Faults %s" % (faults))
    cat_map = dict([(x[0], idx) for idx, x in enumerate(categories)])
except Exception, err:
    msg = "show_jobs %s" % err
    critical_exit(msg)

if len(go.options.users):
    # remove all non-listed users
    for user in ustats.keys():
        if not user in go.options.users:
            del ustats[user]

agg_ans = [0] * (len(categories) - 1) + [[]]
for user, tmp in ustats.items():
    for i in range(len(categories)):
        agg_ans[i] += tmp[i]


if go.options.nagios:
    # maxchars: total should be 80, - 2*6 + 1 ' '
    absmaxchars = 200
    maxuserchars = 20
    msg = NagiosResult('show_jobs')

    for i in ['R', 'Q', 'RN', 'RC', 'RP', 'QN', 'QC', 'QP']:
        setattr(msg, i, agg_ans[cat_map[i]])
    msg.O = len(agg_ans[cat_map['O']])
    msg.QP /= 3600
    msg.RP /= 3600
    msg.running = agg_ans[0]
    msg.queued = agg_ans[4]
    # users with Running jobs
    msg.RU = sum([x[cat_map['R']] > 0 for x in ustats.values()])
    # users with Queued jobs
    msg.QU = sum([x[cat_map['Q']] > 0 for x in ustats.values()])
    # unique users
    msg.UU = sum([x[cat_map['R']] + x[cat_map['Q']] > 0 for x in ustats.values()])

    ok_exit(msg)

else:
    txt = []
    run_values = (agg_ans[cat_map['R']], agg_ans[cat_map['RN']], agg_ans[cat_map['RC']], int(agg_ans[cat_map['RP']] / 3600),)
    txt.append("%s running jobs on %s nodes (%s cores, %s prochours)" % run_values)
    queued_values = (agg_ans[cat_map['Q']], agg_ans[cat_map['QN']], agg_ans[cat_map['QC']], int(agg_ans[cat_map['QP']] / 3600),)
    txt.append("%s queued jobs for %s nodes (%s cores, %s prochours)" % queued_values)

    others = agg_ans[cat_map['O']]
    if  len(others) > 0:
        txt.append("Other jobs: %s (%s)" % (len(others), ','.join(others)))

    if go.options.detailed:
        indent = "  "
        users = ustats.keys()
        for user in sorted(users):
            txt.append("%s%s" % (indent, user))
            ans = ustats[user]
            run_values = (indent * 2, ans[cat_map['R']], ans[cat_map['RN']], ans[cat_map['RC']], int(ans[cat_map['RP']] / 3600),)
            txt.append("%s%s running jobs on %s nodes (%s cores, %s prochours)" % run_values)
            queued_values = (indent * 2, ans[cat_map['Q']], ans[cat_map['QN']], ans[cat_map['QC']], int(ans[cat_map['QP']] / 3600),)
            txt.append("%s%s queued jobs for %s nodes (%s cores, %s prochours)" % queued_values)

            others = ans[cat_map['O']]
            if  len(others) > 0:
                txt.append("%sOther jobs: %s (%s)" % (indent * 2, len(others), ','.join(others)))

    print "\n".join(txt)
    sys.exit(0)
