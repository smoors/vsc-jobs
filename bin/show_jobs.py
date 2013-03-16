#!/usr/bin/python

"""
Show job information
"""
import grp
import sys

from vsc.utils.generaloption import simple_option
from vsc.jobs.pbs.jobs import get_userjob_stats

options = {
           'users':('Report for users', None, "extend", [], 'u'),
           'groups':('Report for groups', None, "extend", [], 'g'),
           'detailed':('Report detailed information', None, 'store_true', False, 'D'),
           'nagios':('Report in nagios format', None, 'store_true', False, 'n'),
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
    if go.options.nagios:
        msg = "show_jobs CRITICAL %s" % err
    else:
        msg = "show_jobs %s" % err
    print msg
    sys.exit(2)

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

    # sort by queued jobs (index 3)
    uns = [(tmp[cat_map['Q']], user) for user, tmp in ustats.items()]
    users_by_queued = [y for x, y in sorted(uns, reverse=True)]
    restr = 0
    restq = 0
    runuser = 0
    queueuser = 0
    uniqueuser = 0
    txt = ''
    for idx, user in enumerate(users_by_queued):
        if ustats[user][cat_map['R']] > 0:
            runuser += 1
        if ustats[user][cat_map['Q']] > 0:
            queueuser += 1
        if ustats[user][cat_map['R']] + ustats[user][cat_map['Q']] > 0:
            uniqueuser += 1

        if len(txt) > absmaxchars - 2 * maxuserchars:
            restr += ustats[user][cat_map['R']]
            restq += ustats[user][cat_map['Q']]
            continue

        txt = "r_%s=%s q_%s=%s %s" % (idx, ustats[user][cat_map['R']], idx, ustats[user][cat_map['Q']], txt)
    if (restr + restq) > 0:
        txt = "r_rest=%s q_rest=%s %s" % (restr, restq, txt)

    header = "show_jobs OK"
    summ = "Running:%s Queued:%s" % (agg_ans[0], agg_ans[4])
    summary = "R=%s Q=%s O=%s RN=%s RC=%s RP=%s QN=%s QC=%s QP=%s RU=%s QU=%s UU=%s"

    values = (
              agg_ans[cat_map['R']], agg_ans[cat_map['Q']], agg_ans[cat_map['O']],
              agg_ans[cat_map['RN']], agg_ans[cat_map['RC']], int(agg_ans[cat_map['RP']] / 3600),
              agg_ans[cat_map['QN']], agg_ans[cat_map['QC']], int(agg_ans[cat_map['QP']] / 3600),
              runuser, queueuser, uniqueuser,
              )
    # # too much chars for icinga/ido2db
    txt = ''
    msg = "%s - %s | %s %s" % (header, summ, summary % values, txt)

    print msg
    sys.exit(0)
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
