"""
@author: Jens Timmerman
"""

import glob
import os
import sys

import pprint
from difflib import ndiff

from unittest import TestCase, TestLoader, main
from vsc.jobs.pbs.submitfilter import SubmitFilter, get_warnings, reset_warnings
from vsc.jobs.pbs.clusterdata import DEFAULT_SERVER_CLUSTER

from vsc.utils.run import run_simple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'bin')))
import submitfilter

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

    def assertEqual(self, a, b, msg=None):
        try:
            super(TestSubmitfilter, self).assertEqual(a, b)
        except AssertionError as e:
            if msg is None:
                msg = str(e)
            else:
                msg = "%s: %s" % (msg, e)

            if isinstance(a, basestring):
                txta = a
            else:
                txta = pprint.pformat(a)
            if isinstance(b, basestring):
                txtb = b
            else:
                txtb = pprint.pformat(b)

            # max 20 lines
            diff = list(ndiff(txta.splitlines(1), txtb.splitlines(1)))[:12]

            raise AssertionError("%s:\n%s" % (msg, ''.join(diff)))

    def setUp(self):
        reset_warnings()
        for env in ['PBS_DEFAULT', 'PBS_DPREFIX', 'VSC_NODE_PARTITION']:
            if env in os.environ:
                del os.environ[env]

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
                         '#PBS -o output_testrun.txt -l nodes=5:ppn=16,pmem=33815203840',
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
            '#PBS -l vmem=4897988864',
            '# Adding PARTTION as specified in VSC_NODE_PARTITION',
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
            '#PBS -l vmem=78367821824',
            '#PBS -l pmem=33815203840',
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
            'Warning, requested 1099511627776b vmem per node, this is more then the available vmem (88905359360b), this job will never start.',
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
            cmd = "python -B %s" % submitfilter.__file__
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


def suite():
    """ return all the tests"""
    return TestLoader().loadTestsFromTestCase(TestSubmitfilter)

if __name__ == '__main__':
    """Use this __main__ block to help write and test unittests
        just uncomment the parts you need
    """
    main()
