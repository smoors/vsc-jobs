"""
@author: stdweird
"""
from mock import patch

import os
import sys
from vsc.install.testing import TestCase

from vsc.jobs.pbs.jobs import get_jobs_dict


class JobData(dict):
    """Mocked job class to mimick the return value of get_query"""

    def get_nodes(self):
        return self.get('exec_host')


def wrap_jobdata(jobs):
    """Wrap each job in jobs dict in JobData"""
    res = {}
    for jobid, value in jobs.items():
        jd = JobData()
        jd.update(value)
        res[jobid] = jd
    return res


class TestJobs(TestCase):
    def test_get_jobs_dict(self):
        """Test get_jobs_dict"""

        # output from
        #     python -c 'import pprint;from vsc.jobs.pbs.jobs import get_jobs;a={};eval("a.update(%s)" % get_jobs());pprint.pprint(a)'
        # The jobdata however should be PBS Query instances, and get_jobs_dict uses get_nodes() method call

        testdata = os.path.join(os.path.dirname(__file__), 'testpbs', 'pbs_get_jobs_data_master21_t51')
        jobs = {}
        eval("jobs.update(%s)" % open(testdata).read().replace("\n",''))

        with patch('vsc.jobs.pbs.jobs.get_jobs', return_value=wrap_jobdata(jobs)):
            res = get_jobs_dict()
            print res
            sids = sorted(res.keys())

            self.assertEqual(len(res), 6, msg='6 jobs found')
            self.assertEqual([res[x]['job_state'][0] for x in sids], ['R', 'R', 'Q', 'Q', 'Q', 'Q'], msg='jobs in expected states')

            self.assertEqual(res[sids[0]]['derived'], {
                'cores': 20,
                'exec_hosts': {'node2617.swalot.gent.vsc': 1},
                'nodes': 1,
                'state': 'R',
                'totalwalltimesec': 14400,
                'used_cput': 6751,
                'used_mem': 24045981696,
                'used_vmem': 25629622272,
                'used_walltime': 344,
                'user': 'vsc40075',
            }, msg='first job has expected derived data')
