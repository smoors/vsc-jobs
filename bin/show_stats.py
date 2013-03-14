#!/usr/bin/python

# # this does something interesting with maui showstats and diagnose
import sys
from vsc import fancylogger
from vsc.utils.generaloption import simple_option
from vsc.jobs.pbs.moab import showstats

_log = fancylogger.getLogger('show_stats')

options = {
           'detailed':('Report detailed information', None, 'store_true', False, 'D'),
           'nagios':('Report in nagios format', None, 'store_true', False, 'n'),
           }

go = simple_option(options)

try:
    stats = showstats()
    summary_stats = stats['summary']
except Exception, err:
    _log.error("Getting showstats failed with error %s" % (err))
    sys.exit(2)

if go.options.nagios:
    header = "show_stats OK"
    summ = "short %.3f long %.3f" % (summary_stats['STE'], summary_stats['LTE'])
    summary = "STE=%.3f LTE=%.3f DPH=%s TPH=%s AP=%s TP=%s"
    values = [summary_stats[x] for x in ['STE', 'LTE', 'DPH', 'TPH', 'CAP', 'CTP']]

    # first 2 in
    msg = "%s - %s | %s" % (header, summ, summary)

    print msg
    sys.exit(0)
else:
    msg = []
    msg.append("Shortterm/Longterm efficiency %.3f/%.3f\n" % (summary_stats['STE'], summary_stats['LTE']))
    msg.append("Dedicate/total prochours %s/%s\n" % (summary_stats['DPH'], summary_stats['TPH']))
    msg.append("Active/Total procs %s/%s" % (summary_stats['CAP'], summary_stats['CTP']))

    print "\n".join(msg)
    sys.exit(0)

