#
# Copyright 2016-2016 Ghent University
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
@author: stdweird
"""
from mock import patch

import os
import sys
from vsc.install.testing import TestCase

from vsc.jobs.moab.checkjob import SshCheckjob


class TestSshCheckjob(TestCase):
    def test_sshcheckjob(self):
        """Test sshcheckjob"""


        clusters = {'delcatty': {'path': '/opt/moab/bin/checkjob', 'master': 'master15.delcatty.gent.vsc'}, 'phanpy': {'path': '/opt/moab/bin/checkjob', 'master': 'master17.phanpy.gent.vsc'}, 'raichu': {'path': '/opt/moab/bin/checkjob', 'master': 'master13.raichu.gent.vsc'}, 'golett': {'path': '/opt/moab/bin/checkjob', 'master': 'master19.golett.gent.vsc'}, 'swalot': {'path': '/opt/moab/bin/checkjob', 'master': 'master21.swalot.gent.vsc'}}

        checkjob = SshCheckjob(
            'master1',
            'testuser',
            clusters=clusters,
            cache_pickle=True,
            dry_run=True)
