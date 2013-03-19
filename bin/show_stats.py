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
The script prints moab/maui scheduler details like teh showstats command.

@author: Stijn De Weirdt (Ghent University)
"""

# this does something interesting with maui showstats and diagnose
import sys
from vsc import fancylogger
from vsc.utils.generaloption import simple_option
from vsc.jobs.pbs.moab import showstats

_log = fancylogger.getLogger('show_stats')

options = {
           'detailed':('Report detailed information', None, 'store_true', False, 'D'),
           'nagios':('Report in nagios format', None, 'store_true', False, 'n'),
           'moabxml':('Use xml moab data from file (for testing)', None, 'store', None),
           }

go = simple_option(options)

if go.options.moabxml:
    try:
        moabxml = open(go.options.moabxml).read()
    except:
        _log.error('Failed to read moab xml from %s' % go.options.moabxml)
else:
    moabxml = None

try:
    stats = showstats(xml=moabxml)
    summary_stats = stats['summary']
except Exception, err:
    _log.error("Getting showstats failed with error %s" % (err))
    sys.exit(2)

if go.options.nagios:
    header = "show_stats OK"
    summ = "short %.3f long %.3f" % (summary_stats['STE'], summary_stats['LTE'])
    summary = "STE=%.1f LTE=%.1f DPH=%.0f TPH=%.0f AP=%s TP=%s"
    values = tuple([summary_stats[x] for x in ['STE', 'LTE', 'DPH', 'TPH', 'CAP', 'CTP']])

    # first 2 in
    msg = "%s - %s | %s" % (header, summ, summary % values)

    print msg
    sys.exit(0)
else:
    msg = []
    msg.append("Shortterm/Longterm efficiency %.3f/%.3f\n" % (summary_stats['STE'], summary_stats['LTE']))
    msg.append("Dedicate/total prochours %s/%s\n" % (summary_stats['DPH'], summary_stats['TPH']))
    msg.append("Active/Total procs %s/%s" % (summary_stats['CAP'], summary_stats['CTP']))

    print "\n".join(msg)
    sys.exit(0)

