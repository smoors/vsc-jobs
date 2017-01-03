#
# Copyright 2013-2017 Ghent University
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
Some convenient tools

@author: Stijn De Weirdt (Ghent University)
"""
import re
from vsc.utils import fancylogger

_log = fancylogger.getLogger('pbs.tools', fname=False)

# units
UNIT_PREFIX = ['', 'k', 'm', 'g', 't']
UNITS_LOWER = ['%sb' % x for x in UNIT_PREFIX]
UNIT_REG = re.compile(r'^\s*(?P<value>\d+)?(?P<unit>%s)?\s*$' % '|'.join(UNITS_LOWER), re.I)

# DD:HH:MM:SS regexp
TIME_REG = re.compile(r"((((?P<day>\d+):)?(?P<hour>\d+):)?(?P<min>\d+):)?(?P<sec>\d+)")


def str2byte(txt):
    """Simple conversion of string to integer as per units used in pbs"""
    r = UNIT_REG.search(txt)
    if r is None:
        return None

    value = r.group('value')
    if value is None:
        value = 1
    else:
        value = int(value)

    unit = r.group('unit')
    if unit is None:
        unit = 'b'
    unit_int = 1024 ** UNITS_LOWER.index(unit.lower())

    return value * unit_int


def str2sec(txt):
    """Convert a DD:HH:MM:SS format to seconds"""
    m = TIME_REG.search(txt)
    if m:
        totalwallsec = int(m.group('sec'))
        if m.group('min'):
            totalwallsec += int(m.group('min')) * 60
            if m.group('hour'):
                totalwallsec += int(m.group('hour')) * 60 * 60
                if m.group('day'):
                    totalwallsec += int(m.group('day')) * 60 * 60 * 24
        return totalwallsec
    else:
        return None


