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
from vsc.jobs.pbs.moab import showstats
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

logger = fancylogger.getLogger('show_stats')


def main():
    """The script"""

    options = {
            'detailed': ('Report detailed information', None, 'store_true', False, 'D'),
            'moabxml': ('Use xml moab data from file (for testing)', None, 'store', None),
            }

    opts = ExtendedSimpleOption(options)

    msg = "show_stats completed"
    try:
        if opts.options.moabxml:
            try:
                moabxml = open(opts.options.moabxml).read()
            except:
                logger.error('Failed to read moab xml from %s' % opts.options.moabxml)
        else:
            moabxml = None

        moab_stats = showstats(xml=moabxml)

        if not moab_stats:
            logger.error("Moab's showstats dit not provide useful output, likely timed out.")
            opts.critical("Moab's showstats failed running correctly")
            sys.exit(NAGIOS_EXIT_CRITICAL)

        else:
            stats = moab_stats['summary']

            if opts.options.detailed:
                detailed_info_string = """Shortterm/Longterm efficiency %.3f/%.3f
Dedicate/total prochours %s/%s
Active/Total procs %s/%s""" % (stats['STE'], stats['LTE'],
                               stats['DPH'], stats['TPH'],
                               stats['CAP'], stats['CTP'],)
                logger.info("detailed result STE = %s LTE = %s DPH = %s CAP = %s CTP = %s" %
                            (stats['STE'], stats['LTE'],
                             stats['DPH'], stats['TPH'],
                             stats['CAP'], stats['CTP'],))
                print detailed_info_string

            info_string = "short %.3f long %.3f" % (stats['STE'], stats['LTE'])
            logger.info("result: %s" % (info_string,))
            msg += " %s" % (info_string,)
    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue(msg, stats)


if __name__ == '__main__':
    main()
