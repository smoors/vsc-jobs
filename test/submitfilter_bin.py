#
# Copyright 2016-2018 Ghent University
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
@author: Jens Timmerman
"""

import glob
import os
import sys
import re
import mock

import submitfilter

from vsc.install.shared_setup import vsc_setup
from vsc.install.testing import TestCase
from vsc.jobs.pbs.submitfilter import SubmitFilter, get_warnings, reset_warnings, MEM_REGEXP
from vsc.utils.run import run_simple

REPO_BASE_DIR = vsc_setup().REPO_BASE_DIR

SCRIPTS = [
# 0
"""#!/bin/sh
#
#
#PBS -N testrun
#PBS -o output_testrun.txt -l nodes=5:ppn=all,pmem=half
#PBS -e error_testrun.txt
#PBS -l walltime=11:25:00
#PBS -l pvmem=500mb
#PBS -m bea
#PBS -q short
#
cd $VSC_HOME
##logs to stderr by default, redirect this to stdout
./pfgw64s 42424242_1t.txt 2>> $VSC_SCRATCH/testrun.42424242.out
""",

# 1
"""#!/bin/bash
hostname
""",

# 2
"""#!/bin/bash
#PBS -l nodes=3:ppn=half
#PBS -l vmem=full
#PBS -l pmem=half
#PBS -m n
whatever
""",

# 3
"""#!/bin/bash
#PBS -q short@master19.golett.gent.vsc
#PBS -l nodes=1:ppn=4
#PBS -l vmem=1tb
#PBS -m bea
whatever
""",

# 4  -- requesting mem
""" #!/bin/bash
#PBS -l nodes=1:ppn=4
#PBS -l mem=4g
#PBS -m n
""",

# 5
""" #!/bin/bash
        #PBS -l nodes=1:ppn=4
#PBS -l vmem=1g
#PBS -m n
""",

# 6  -- requesting pmem
""" #!/bin/bash
#PBS -l nodes=1:ppn=4
#PBS -l pmem=1g
#PBS -m n
""",


]


class TestSubmitfilter(TestCase):

    def setUp(self):
        reset_warnings()
        for env in ['PBS_DEFAULT', 'PBS_DPREFIX',
                    submitfilter.ENV_NODE_PARTITION, submitfilter.ENV_RESERVATION]:
            if env in os.environ:
                del os.environ[env]
        super(TestSubmitfilter, self).setUp()

    def test_make_new_header_basic(self):
        """Basic test for make_new_header"""
        sf = SubmitFilter(
            ['-q', 'verylong'],
            [x + "\n" for x in SCRIPTS[0].split("\n")]
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
          add pvmem unless defined
          VSC_NODE_PARTITION
        """

        partname = 'mypartition'
        os.environ[submitfilter.ENV_NODE_PARTITION] = partname

        reserv = 'magicreserv'
        os.environ[submitfilter.ENV_RESERVATION] = reserv

        sf = SubmitFilter(
            [],
            [x + "\n" for x in SCRIPTS[1].split("\n")]
        )
        sf.parse_header()

        self.assertEqual(sf.header, ['#!/bin/bash'], msg='minimal header from minimal script')
        header = submitfilter.make_new_header(sf)
        self.assertEqual(header, [
            '#!/bin/bash',
            '# No mail specified - added by submitfilter',
            '#PBS -m n',
            '# No pmem or vmem limit specified - added by submitfilter (server found: delcatty)',
            '#PBS -l vmem=4720302336',
            '# Adding PARTITION as specified in VSC_NODE_PARTITION',
            '#PBS -W x=PARTITION:%s' % partname,
            '# Adding reservation as specified in VSC_RESERVATION',
            '#PBS -W x=FLAGS:ADVRES:%s' % reserv,
        ], msg='added missing defaults and pratiton information to header')

        del os.environ[submitfilter.ENV_NODE_PARTITION]
        del os.environ[submitfilter.ENV_RESERVATION]

    def test_make_new_header(self):
        """Test make_new_header resource replacement"""
        sf = SubmitFilter(
            [],
            [x + "\n" for x in SCRIPTS[2].split("\n")]
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

    def test_make_new_header_with_existing_mem(self):
        sf = SubmitFilter(
            [],
            [x + "\n" for x in SCRIPTS[4].split("\n")]
        )
        sf.parse_header()
        header = submitfilter.make_new_header(sf)
        self.assertEqual(header, [
            '#!/bin/bash',
            '#PBS -l nodes=1:ppn=4',
            '#PBS -l mem=4g',
            '#PBS -m n'
            '',
            '',
        ], msg='header with existing mem set')

    @mock.patch('submitfilter.get_clusterdata')
    @mock.patch('submitfilter.get_cluster_overhead')
    def test_make_new_header_mem_limits(self, mock_cluster_overhead, mock_clusterdata):
        reset_warnings()
        sf = SubmitFilter(
            [],
            [x + "\n" for x in SCRIPTS[4].split("\n")]  # requesting mem example
        )

        mock_clusterdata.return_value = {
            'TOTMEM': 4 << 30,
            'PHYSMEM': 1024 << 20,
            'NP': 8,
            'NP_LCD': 2,
        }
        mock_cluster_overhead.return_value = 0

        sf.parse_header()
        header = submitfilter.make_new_header(sf)

        # header should not change
        self.assertEqual(header, [
            '#!/bin/bash',
            '#PBS -l nodes=1:ppn=4',
            '#PBS -l mem=4g',
            '#PBS -m n'
            '',
            '',
        ], msg='header with existing mem set')
        self.assertEqual(get_warnings(), [
            "Unable to determine clustername, using default delcatty (no PBS_DEFAULT)",
            "Warning, requested %sb mem per node, this is more than the available mem (%sb), this job will never start." % (4 << 30, 1024 << 20 )
        ])

    @mock.patch('submitfilter.get_clusterdata')
    @mock.patch('submitfilter.get_cluster_overhead')
    def test_make_new_header_pmem_limits(self, mock_cluster_overhead, mock_clusterdata):
        sf = SubmitFilter(
            [],
            [x + "\n" for x in SCRIPTS[6].split("\n")]
        )

        mock_clusterdata.return_value = {
            'TOTMEM': 4096 << 20,
            'PHYSMEM': 3072 << 20,
            'NP': 8,
            'NP_LCD': 2,
        }
        mock_cluster_overhead.return_value = 0

        sf.parse_header()
        header = submitfilter.make_new_header(sf)
        self.assertEqual(header, [
            '#!/bin/bash',
            '#PBS -l nodes=1:ppn=4',
            '#PBS -l pmem=1g',
            '#PBS -m n'
            '',
            '',
        ], msg='header with existing mem set')
        self.assertEqual(get_warnings(), [
            "Unable to determine clustername, using default delcatty (no PBS_DEFAULT)",
            "Warning, requested %sb pmem per node, this is more than the available pmem (%sb), this job will never start." % (1 << 30, (3072 << 20) / 8 )
        ])

    def test_make_new_header_ignore_indentation(self):
        sf = SubmitFilter(
            [],
            [x + "\n" for x in SCRIPTS[5].split("\n")]
        )
        sf.parse_header()
        header = submitfilter.make_new_header(sf)
        self.assertEqual(header, [
            '#!/bin/bash',
            '#PBS -l nodes=1:ppn=4',
            '#PBS -l vmem=1g',
            '#PBS -m n',
            '',
        ], msg='header with an indented line')

    def test_make_new_header_warn(self):
        """
        Test make_new_header warnings
            ideal ppn
            vmem too high
        """
        reset_warnings()

        sf = SubmitFilter(
            [],
            [x + "\n" for x in SCRIPTS[3].split("\n")]
        )
        sf.parse_header()

        header = submitfilter.make_new_header(sf)
        self.assertEqual(header, sf.header, msg='unmodified header')
        self.assertEqual(get_warnings(), [
            'The chosen ppn 4 is not considered ideal: should use either lower than or multiple of 3',
            'Warning, requested 1099511627776b vmem per node, this is more than the available vmem (86142287872b), this job will never start.',
        ], msg='warnings for ideal ppn and vmem too high')

    def test_run_subshell(self):
        """Read data from testjobs_submitfilter and feed it through submitfilter script"""

        testdir = os.path.join(os.path.dirname(__file__), 'testjobs_submitfilter')
        for scriptfn in glob.glob("%s/*.script" % testdir):
            scriptname = os.path.basename(scriptfn)
            name = '.'.join(scriptname.split('.')[:-1])

            script = os.path.join(testdir, scriptname)
            out = os.path.join(testdir, "%s.out" % name)
            err = os.path.join(testdir, "%s.err" % name)
            log = os.path.join(testdir, "%s.log" % name)
            cmdline = os.path.join(testdir, "%s.cmdline" % name)

            # avoid pyc files in e.g. bin
            cmd = 'PYTHONPATH="%s:$PYTHONPATH" ' % os.pathsep.join([p for p in sys.path if p.startswith(REPO_BASE_DIR)])
            cmd += "python -B '%s'" % submitfilter.__file__
            if os.path.exists(cmdline):
                cmd += " " + open(cmdline).readline().strip()

            # make command
            # get output, and exitcode
            ec, output = run_simple(cmd, input=open(script).read())

            self.assertEqual(ec, 0, msg="submitfilter ended with ec 0 for script %s and cmdline %s" % (name, cmd))

            res = ''
            if os.path.exists(log):
                # multiline pattern match, line per line
                for pattern in open(log).readlines():
                    if not pattern or pattern.startswith('#'):
                        continue
                    reg = re.compile(r''+pattern, re.M)
                    if reg.search(output):
                        output = reg.sub('', output)
                    else:
                        self.assertTrue(False, "Expected a log pattern match %s for script %s" % (pattern, name))

            if os.path.exists(out):
                res += open(out).read()
            else:
                self.assertTrue(False, msg='no output file %s for script %s' % (out, name))

            if os.path.exists(err):
                res += open(err).read()

            self.assertEqual(output, res, msg="expected output for script %s and cmdline %s" % (name, cmd))

    def test_mem_regex(self):
        """
        See if the regex matches properly
        """
        self.assertFalse(MEM_REGEXP.search("vmem") is None)
        self.assertFalse(MEM_REGEXP.search("pmem") is None)
        self.assertFalse(MEM_REGEXP.search("pvmem") is None)
        self.assertFalse(MEM_REGEXP.search("mem") is None)

        self.assertTrue(MEM_REGEXP.search("vvmem") is None)
        self.assertTrue(MEM_REGEXP.search("pvvmem") is None)
        self.assertTrue(MEM_REGEXP.search("vpvmem") is None)
        self.assertTrue(MEM_REGEXP.search("rmem") is None)
