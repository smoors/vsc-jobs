"""
@author: stdweird
"""
import os
import sys
from unittest import TestCase, TestLoader, main

from vsc.jobs.pbs.clusterdata import get_clusterdata, \
    get_cluster_maxppn, get_cluster_mpp, \
    MIN_VMEM, DEFAULT_SERVER_CLUSTER, DEFAULT_SERVER, CLUSTERDATA

# insert test cluster to test
CLUSTERDATA['zzzmytest'] = {
    'PHYSMEM': 99189980 << 10,  # 94.0GB
    'TOTMEM': 120161496 << 10,  # 114.0GB
    'DEFMAXNP': 48,
}

SORTED_CLUSTERS = ['delcatty', 'dugtrio', 'gastly', 'gengar', 'golett', 'gulpin', 'haunter', 'muk', 'phanpy', 'raichu']

class TestPbsClusterdata(TestCase):
    def test_consts(self):
        """The constants should change by accident"""
        self.assertEqual(MIN_VMEM, 1536 * 1024 * 1024, msg="MIN_VMEM as expected")
        self.assertEqual(DEFAULT_SERVER_CLUSTER, 'delcatty', msg="DEFAULT_SERVER_CLUSTER, as expected")

        self.assertTrue(DEFAULT_SERVER_CLUSTER in CLUSTERDATA, msg='DEFAULT_SERVER_CLUSTER in CLUSTERDATA')

        self.assertEqual(DEFAULT_SERVER, 'default', msg="DEFAULT_SERVER as expected")
        self.assertEqual(sorted(CLUSTERDATA.keys()),
                         SORTED_CLUSTERS + ['zzzmytest'],
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

    def test_mpp(self):
        """Test get_cluster_vpp"""
        for cl, mpp in [('delcatty', (4226900480, 4897988864)), ('zzzmytest', (2116052906, 2339749077))]:
            self.assertEqual(get_cluster_mpp(cl), mpp, msg="expected mpp %s for %s" % (mpp, cl,))


def suite():
    """ return all the tests"""
    return TestLoader().loadTestsFromTestCase(TestPbsClusterdata)

if __name__ == '__main__':
    """Use this __main__ block to help write and test unittests
        just uncomment the parts you need
    """
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    main()
