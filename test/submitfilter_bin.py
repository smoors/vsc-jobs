#
# Copyright 2016-2016 Ghent University
#
# This file is part of vsc-jobs,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
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
@author: Jens Timmerman
"""

import glob
import os
import sys

import pprint
import submitfilter

from vsc.install.shared_setup import REPO_BASE_DIR
from vsc.install.testing import TestCase
from vsc.jobs.pbs.submitfilter import SubmitFilter, get_warnings, reset_warnings
from vsc.jobs.pbs.clusterdata import DEFAULT_SERVER_CLUSTER
from vsc.utils.run import run_simple


SCRIPTS = ["""#!/bin/sh
#
#
#PBS -N testrun
#PBS -o output_testrun.txt -l nodes=5:ppn=all,pmem=half
#PBS -e error_testrun.txt
#PBS -l walltime=11:25:00
#PBS -l vmem=500mb
#PBS -m bea
#PBS -q short
#
cd $VSC_HOME
##logs to stderr by default, redirect this to stdout
./pfgw64s 42424242_1t.txt 2>> $VSC_SCRATCH/testrun.42424242.out
""",

"""#!/bin/bash
hostname
""",
"""#!/bin/bash
#PBS -l nodes=3:ppn=half
#PBS -l vmem=full
#PBS -l pmem=half
#PBS -m n
whatever
""",
"""#!/bin/bash
#PBS -q short@master19.golett.gent.vsc
#PBS -l nodes=1:ppn=4
#PBS -l vmem=1tb
#PBS -m bea
whatever
"""
]

class TestSubmitfilter(TestCase):

    def setUp(self):
        reset_warnings()
        for env in ['PBS_DEFAULT', 'PBS_DPREFIX', 'VSC_NODE_PARTITION']:
            if env in os.environ:
                del os.environ[env]
        super(TestSubmitfilter, self).setUp()

    def test_make_new_header_basic(self):
        """Basic test for make_new_header"""
        sf = SubmitFilter(
            ['-q', 'verylong'],
            [ x + "\n" for x in SCRIPTS[0].split("\n")]
        )
        sf.parse_header()

        old_header = sf.header[:]
        header = submitfilter.make_new_header(sf)
        self.assertEqual(old_header, sf.header, msg='make_new_header leaves original unmodified')

        modresourcesidx = 4

        self.assertEqual(header.pop(modresourcesidx),
                         '#PBS -o output_testrun.txt -l nodes=5:ppn=16,pmem=32393711616',
                         msg='replace the resource header as expected')

        sf.header.pop(modresourcesidx)
        self.assertEqual(header, sf.header, msg='all other header lines unmodified')

    def test_make_new_header_add_missing(self):
        """
        Test make_new_header
          add missing mail / unless present
          add vmem unless defined
          VSC_NODE_PARTITION
        """

        partname = 'mypartition'
        os.environ['VSC_NODE_PARTITION'] = partname
        sf = SubmitFilter(
            [],
            [ x + "\n" for x in SCRIPTS[1].split("\n")]
        )
        sf.parse_header()

        self.assertEqual(sf.header, ['#!/bin/bash'], msg='minimal header from minimal script')
        header = submitfilter.make_new_header(sf)
        self.assertEqual(header, [
            '#!/bin/bash',
            '# No mail specified - added by submitfilter',
            '#PBS -m n',
            '# No vmem limit specified - added by submitfilter (server found: delcatty)',
            '#PBS -l vmem=4720302336',
            '# Adding PARTITION as specified in VSC_NODE_PARTITION',
            '#PBS -W x=PARTITION:%s' % partname,
        ], msg='added missing defaults and pratiton information to header')

        del os.environ['VSC_NODE_PARTITION']

    def test_make_new_header(self):
        """Test make_new_header resource replacement"""
        sf = SubmitFilter(
            [],
            [ x + "\n" for x in SCRIPTS[2].split("\n")]
        )
        sf.parse_header()

        header = submitfilter.make_new_header(sf)
        self.assertEqual(header, [
            '#!/bin/bash',
            '#PBS -l nodes=3:ppn=8',
            '#PBS -l vmem=75524837376',
            '#PBS -l pmem=32393711616',
            '#PBS -m n',
        ], msg='modified header with resources replaced')

    def test_make_new_header_warn(self):
        """
        Test make_new_header warnings
            ideal ppn
            vmem too high
        """
        reset_warnings()

        sf = SubmitFilter(
            [],
            [ x + "\n" for x in SCRIPTS[3].split("\n")]
        )
        sf.parse_header()

        header = submitfilter.make_new_header(sf)
        self.assertEqual(header, sf.header, msg='unmodified header')
        self.assertEqual(get_warnings(), [
            'The chosen ppn 4 is not considered ideal: should use either lower than or multiple of 3',
            'Warning, requested 1099511627776b vmem per node, this is more than the available vmem (86142287872b), this job will never start.',
        ], msg='warnings for ideal ppn and vmmem too high')


    def test_run_subshell(self):
        """Read data from testjobs_submitfilter and feed it through submitfilter script"""

        testdir = os.path.join(os.path.dirname(__file__), 'testjobs_submitfilter')
        for scriptfn in glob.glob("%s/*.script" % testdir):
            scriptname = os.path.basename(scriptfn)
            name = '.'.join(scriptname.split('.')[:-1])

            script = os.path.join(testdir, scriptname)
            out = os.path.join(testdir, "%s.out" % name)
            err = os.path.join(testdir, "%s.err" % name)
            cmdline = os.path.join(testdir, "%s.cmdline" % name)

            # avoid pyc files in e.g. bin
            cmd = 'PYTHONPATH=%s ' % os.pathsep.join([p for p in sys.path if p.startswith(REPO_BASE_DIR)])
            cmd += "python -B %s" % submitfilter.__file__
            if os.path.exists(cmdline):
                cmd += " " + open(cmdline).readline().strip()

            # make command
            # get output, and exitcode
            ec, output = run_simple(cmd, input=open(script).read())

            self.assertEqual(ec, 0, msg="submitfiler ended with ec 0 for script %s and cmdline %s" % (name, cmd))

            res = ''
            if os.path.exists(out):
                res += open(out).read()
            else:
                self.assertTrue(False, msg='no output file %s for script %s' % (out, name))

            if os.path.exists(err):
                res += open(err).read()
            
            self.assertEqual(output, res, msg="expected output for script %s and cmdline %s" % (name, cmd))
