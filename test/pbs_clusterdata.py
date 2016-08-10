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
import os
import sys
from vsc.install.testing import TestCase

from vsc.jobs.pbs.clusterdata import get_clusterdata, \
    get_cluster_maxppn, get_cluster_mpp, get_cluster_overhead, \
    MIN_VMEM, DEFAULT_SERVER_CLUSTER, DEFAULT_SERVER, CLUSTERDATA

SORTED_CLUSTERS = ['banette', 'delcatty', 'golett', 'muk', 'phanpy', 'raichu', 'shuppet', 'swalot']

class TestPbsClusterdata(TestCase):
    def setUp(self):
        # insert test cluster to test
        CLUSTERDATA['zzzmytest'] = {
            'PHYSMEM': 99189980 << 10,  # 94.0GB
            'TOTMEM': 120161496 << 10,  # 114.0GB
            'DEFMAXNP': 48,
        }
        CLUSTERDATA['zzzmytestavail'] = {
            'PHYSMEM': 99189980 << 10,  # 94.0GB
            'TOTMEM': 120161496 << 10,  # 114.0GB
            'AVAILMEM': 110161496 << 10, # 105 GB
            'DEFMAXNP': 48,
        }
        super(TestPbsClusterdata, self).setUp()

    def tearDown(self):
        CLUSTERDATA.pop('zzzmytest')
        CLUSTERDATA.pop('zzzmytestavail')
        super(TestPbsClusterdata, self).tearDown()

    def test_consts(self):
        """The constants shouldn't change by accident"""
        self.assertEqual(MIN_VMEM, 1536 * 1024 * 1024, msg="MIN_VMEM as expected")
        self.assertEqual(DEFAULT_SERVER_CLUSTER, 'delcatty', msg="DEFAULT_SERVER_CLUSTER, as expected")

        self.assertTrue(DEFAULT_SERVER_CLUSTER in CLUSTERDATA, msg='DEFAULT_SERVER_CLUSTER in CLUSTERDATA')

        self.assertEqual(DEFAULT_SERVER, 'default', msg="DEFAULT_SERVER as expected")
        self.assertEqual(sorted(CLUSTERDATA.keys()),
                         SORTED_CLUSTERS + ['zzzmytest', 'zzzmytestavail'],
                         msg='sorted clusters from CLUSTERDATA')

    def test_getdata(self):
        """Test parse_resources_nodes"""

        self.assertEqual(get_clusterdata('doesnotexists'), CLUSTERDATA[DEFAULT_SERVER_CLUSTER],
                         msg='get_clusterdata for non-existing cluster returns DEFAULT_SERVER_CLUSTER data')

        for cluster in SORTED_CLUSTERS:
            self.assertEqual(get_clusterdata(cluster), CLUSTERDATA[cluster],
                            msg='get_clusterdata returns correct data for clsuetr %s' %cluster)

    def test_maxppn(self):
        """Test get_cluster_maxppn"""
        for cluster in CLUSTERDATA.keys():
            cd=CLUSTERDATA[cluster]
            self.assertEqual(get_cluster_maxppn(cluster),
                             cd['NP'] if 'NP' in cd else cd['DEFMAXNP'],
                             msg="Found expected maxppn for cluster %s" % cluster)

    def test_overhead(self):
        """Test get_cluster_overhead"""
        self.assertFalse('AVAILMEM' in CLUSTERDATA['zzzmytest'], msg='no AVAILMEM in cluster zzzmytest')
        self.assertEqual(get_cluster_overhead('zzzmytest'), 0, msg="overhead is 0 when no AVAILMEM is defined")

        self.assertTrue('AVAILMEM' in CLUSTERDATA['zzzmytestavail'], msg='AVAILMEM in cluster zzzmytestavail')
        self.assertEqual(get_cluster_overhead('zzzmytestavail'),
                         CLUSTERDATA['zzzmytestavail']['TOTMEM'] - CLUSTERDATA['zzzmytestavail']['AVAILMEM'],
                         msg="overhead is difference between totmem and initial availmem")


    def test_mpp(self):
        """Test get_cluster_vpp"""
        for cl, mpp in [('delcatty', (4049213952, 4720302336)),
                        ('zzzmytest', (2116052906, 2339749077)),
                        ('zzzmytestavail', (1902719573, 2126415744))]:
            self.assertEqual(get_cluster_mpp(cl), mpp, msg="expected mpp %s for %s" % (mpp, cl,))
