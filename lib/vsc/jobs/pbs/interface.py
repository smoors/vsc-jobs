#
# Copyright 2013-2016 Ghent University
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
The main pbs module

@author: Stijn De Weirdt (Ghent University)
"""
from vsc.utils import fancylogger

_log = fancylogger.getLogger('pbs.interface', fname=False)

HAVE_PBS_PYTHON = True
try:
    import pbs
    pbs = pbs  # allow to remote access

    from PBSQuery import PBSQuery
except:
    _log.error('Failed to import pbs_python modules')
    HAVE_PBS_PYTHON = False


def get_query():
    """Return query instance"""
    return PBSQuery()

