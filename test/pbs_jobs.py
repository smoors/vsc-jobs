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

from vsc.jobs.pbs.jobs import DEFAULT_ATTRS, get_jobs, get_jobs_dict


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
    @patch('vsc.jobs.pbs.jobs.pbs')
    @patch('vsc.jobs.pbs.interface.PBSQuery.__init__', return_value=None)
    @patch('vsc.jobs.pbs.interface.PBSQuery.getjobs')
    def test_get_jobs(self, qgj, pbsquery, ppbs):
        """Test get_jobs"""

        def_attrs = ['name', 'owner', 'used', 'state', 'queue', 'session' ,'l',
                      'exechost'] + [x+'time' for x in ['start_', 'c', 'e', 'q', 'm']]
        self.assertEqual(DEFAULT_ATTRS, def_attrs, msg="DEFAULT_ATTRS")

        # default
        get_jobs()
        all_args = qgj.call_args_list
        self.assertEqual(len(all_args), 1, 'getjobs called once');

        args, kwargs = all_args[0]
        self.assertEqual(len(args), 0, 'getjobs called once with no positional args')
        self.assertEqual(len(kwargs), 1, 'getjobs called once with one named arg')
        self.assertEqual(len(kwargs['attrib_list']), len(def_attrs),
                         'getjobs called once with named arg atrrib_list with same size as def_attrs')

        # the pbs ATTR constants are MagicMock instances
        # yuck would be a better name for mock
        mock_txt = str(kwargs['attrib_list'][0])
        self.assertTrue("name='pbs.ATTR_name'" in mock_txt,
                        msg='first value of attrib_list is mocked pbs.ATTR_name constant '+mock_txt)

        # ALL
        qgj.reset_mock()
        get_jobs('ALL')

        all_args = qgj.call_args_list
        self.assertEqual(len(all_args), 1, 'getjobs called once w ALL');

        args, kwargs = all_args[0]
        self.assertEqual(len(args), 0, 'getjobs called once with no positional args w ALL')
        self.assertEqual(len(kwargs), 1, 'getjobs called once with one named arg w ALL')
        self.assertEqual(kwargs['attrib_list'], None,
                         'getjobs called once with named arg atrrib_list None w ALL')

        # non-def
        qgj.reset_mock()
        get_jobs(['some', 'thing'])

        all_args = qgj.call_args_list
        self.assertEqual(len(all_args), 1, 'getjobs called once w non-def');

        args, kwargs = all_args[0]
        self.assertEqual(len(args), 0, 'getjobs called once with no positional args w non-def')
        self.assertEqual(len(kwargs), 1, 'getjobs called once with one named arg w non-def')
        self.assertEqual(len(kwargs['attrib_list']), 2,
                         'getjobs called once with named arg atrrib_list with 2 attrs')

        # the pbs ATTR constants are MagicMock instances
        mock_txt = str(kwargs['attrib_list'][0])
        self.assertTrue("name='pbs.ATTR_some'" in mock_txt,
                        msg='first value of attrib_list is mocked pbs.ATTR_some constant '+mock_txt)


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
