"""
@author: Jens Timmerman
"""

from unittest import TestCase, TestLoader, main
from subprocess import Popen, PIPE
import time
import os
import os.path
import datetime
import signal
import difflib
from pprint import pprint

TIMEOUT = 10

SCRIPTS = ["""#!/bin/sh
#
#
#PBS -N testrun
#PBS -o output_testrun.txt
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
"""#!/bin/sh
#
#
#PBS -N testrun
#PBS -o output_testrun.txt
#PBS -e error_testrun.txt
#PBS -l walltime=11:25:00
#PBS -l nodes=1:ppn=5
#PBS -l vmem=500mb
#PBS -m bea
#PBS -q short
#
cd $VSC_HOME
##logs to stderr by default, redirect this to stdout
./pfgw64s 42424242_1t.txt 2>> $VSC_SCRATCH/testrun.42424242.out
""",
"""#!/bin/sh
#
#
#PBS -N testrun
#PBS -o output_testrun.txt
#PBS -e error_testrun.txt
#PBS -l walltime=11:25:00
#PBS -l vmem=500mb #nodes=1:ppn=5
#PBS -m bea
#PBS -q short
#
cd $VSC_HOME
##logs to stderr by default, redirect this to stdout
./pfgw64s 42424242_1t.txt 2>> $VSC_SCRATCH/testrun.42424242.out
"""
,
"""#!/bin/sh
#PBS -l walltime=11:25:00
##PBS -l vmem=500mb
#PBS -q short
#PBS -m bea
#
""", """#!/bin/sh
#PBS -l walltime=11:25:00
#PBS -l vmem=500
#PBS -q short
#PBS -m bea
#
""",
"""#!/bin/sh
#PBS -l walltime=11:25:00
#PBS -l vmem=500b
#PBS -q short
#PBS -m bea
#
""", """#!/bin/sh
#PBS -l walltime=11:25:00
#PBS -l vmem=500kb
#PBS -q short
#PBS -m bea
#
""", """#!/bin/sh
#PBS -l walltime=11:25:00
#PBS -l vmem=500mb
#PBS -q short
#PBS -m bea
#
""", """#!/bin/sh
#PBS -l walltime=11:25:00
#PBS -l vmem=500gb
#PBS -q short
#PBS -m bea
#
""",
"""#!/bin/sh
#PBS -l walltime=11:25:00
#PBS -l vmem=500w
#PBS -q short
#PBS -m bea
#
""", """#!/bin/sh
#PBS -l walltime=11:25:00
#PBS -l vmem=500kw
#PBS -q short
#PBS -m bea
#
""", """#!/bin/sh
#PBS -l walltime=11:25:00
#PBS -l vmem=500mw
#PBS -q short
#PBS -m bea
#
""", """#!/bin/sh
#PBS -l walltime=11:25:00
#PBS -l vmem=500gw
#PBS -q short
#PBS -m bea
#
"""
]
# test these with the ignore constant, since there's  bugs  in the old filter
SCRIPTS2 = ["""#!/bin/sh
#PBS -l walltime=11:25:00
#PBS -l vmem=500mb
#PBS -q short
#

cd $VSC_HOME
##logs to stderr by default, redirect this to stdout
./pfgw64s 42424242_1t.txt 2>> $VSC_SCRATCH/testrun.42424242.out

#PBS -m bea
""",
"""#!/bin/sh
#
#
#PBS -N testrun
#PBS -o output_testrun.txt
#PBS -e error_testrun.txt
#PBS -l walltime=11:25:00
#PBS -l nodes=1:ppn=5  #blabla vmem=500mb
#PBS -m bea
#PBS -q short
#
cd $VSC_HOME
##logs to stderr by default, redirect this to stdout
./pfgw64s 42424242_1t.txt 2>> $VSC_SCRATCH/testrun.42424242.out
""",
"""#!/bin/sh
#PBS -l walltime=11:25:00
#PBS -m bea
#
cd $VSC_HOME
""",
]

# ignore these lines, they are wrong in the old filter
# TODO: check things without this first
IGNORE = [
         "#PBS -l vmem=20480mb",
         "#PBS -l vmem=28672mb",
         "# No mail specified - added by submitfilter",
         "#PBS -m n",
         ]
        # master[0-9]*\.([^.]*)\.gent\.vsc
SERVERS = ['master1.gengar.gent.vsc',
           'master5.haunter.gent.vsc',
           'master9.gulpin.gent.vsc',
           'master3.gastly.gent.vsc',
           #'master11.dugtrio.gent.vsc', # don't test dugtrio for now, it's a regular batch server
           'master13.raichu.gent.vsc',
           #'master-moab.muk.os', # never worked with old code
           ]
BAD_SERVERS = [
               'nogengar',
               "master1.gengar2.gent.vsc",
               'node035.gengar.gent.vsc',
               'haunterdoesntexist',
               '',
               'blabla',
               'server',
               'master.gent.vsc',
               'master..gent.vsc',
               'master.haunterdoesntexist.gent.vsc',
               'mastergent.vsc',
               ]
GOOD_ARGS = ['',
           "-d 'blabla' jobnaam",
           '-m bea',
           '-l vmem=1000mb',
           '-l  nodes=1:ppn=7',
           '-q short',
           '-q  d short',
           '-q short@gengar',
           '-q @blashort',
           '-q @blashort@gengar',
           '-x ignorethis',
           '--ignorethis',
           '--ignorethis da',
           '-x',
           '-h x',
           ]
BAD_ARGS = [
          '-m',
          '-l',
          ]  # this should not occur, qsub checks this
SUFFIXES_ARGS = [
                 '-l vmem=1000mb',
                 '-l vmem=1000gb',
                 '-l vmem=1000',
                 '-l vmem=1000b',
                 '-l vmem=1000tw',
                 '-l vmem=1000gw',
                 '-l vmem=1000mw',
                 '-l vmem=1000kb',
                 '-l vmem=1000kw',
                 ]

PWD = os.path.dirname(__file__)


def runcmd(cmd):
    start = datetime.datetime.now()
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, close_fds=True)
    timedout = False
    while p.poll() is None:
        time.sleep(0.05)
        if os.path.exists("/proc/%s" % (p.pid)):
            now = datetime.datetime.now()
            if (now - start).seconds > TIMEOUT:
                if timedout == False:
                    os.kill(p.pid, signal.SIGTERM)
                    self.fail("Timeout occured with cmd %s. took more than %i secs to complete." % (cmd, TIMEOUT))
                    timedout = True
                else:
                    os.kill(p.pid, signal.SIGKILL)
    out = p.stdout.read().strip()
    err = p.stderr.read().strip()

    return out, err


class TestLegacySubmitfilter(TestCase):
    def compareResults(self, input, args="", ignore=""):
        old = runcmd("""echo '%s' | python %s/testsubmitfilter/submitfilter %s""" % (input, PWD, args))
        new = runcmd("""echo '%s' | python %s/../bin/submitfilter.py %s""" % (input, PWD, args))
        if new[1]:
            print "stderr new: %s" % new[1]
        if old[1]:
            print "stderr old: %s" % old[1]

        old = old[0].splitlines()
        new = new[0].splitlines()
        pprint (list(difflib.Differ().compare(old, new)))

        # don't fail on '' and #
        new.append('#')
        new.append('')
        new.extend(ignore)
        old.append('#')
        old.append('')
        old.extend(ignore)
        for i in old:
            if not i in new:
                print "%s not in new" % i
                return False
        for i in new:
            if not i in old:
                print "%s not in old" % i
                return False
        return True

    def testCLIOptions(self):
        for arg in GOOD_ARGS:
            msg = "arg %s" % (arg,)
            print msg
            self.assertTrue(self.compareResults("", arg),
                            msg=msg)

    def testempty(self):
        self.assertTrue(self.compareResults(""), msg = "test empty")

    def testscripts(self):
        os.environ['PBS_DEFAULT'] = "zever"
        for script in SCRIPTS:
            del os.environ['PBS_DEFAULT']
            self.assertTrue(self.compareResults(script))
            for i in SERVERS:
                msg = "server PBS_DEFAULT set to %s" %(i,)
                print msg
                os.environ['PBS_DEFAULT'] = i
                self.assertTrue(self.compareResults(script),
                                msg=msg)
            for i in BAD_SERVERS:
                msg = "bad server PBS_DEFAULT set to %s" %(i,)
                print msg
                os.environ['PBS_DEFAULT'] = i
                self.assertTrue(self.compareResults(script),
                                msg=msg)

    def testscripts2(self):
        """
        compares old submitfilter with new one
        but ignores certain bugs in the old script
        """
        os.environ['PBS_DEFAULT'] = "zever"
        for script in SCRIPTS2:
            msg = "script %s" % (script,)
            print msg
            del os.environ['PBS_DEFAULT']
            self.assertTrue(self.compareResults(script, ignore=IGNORE),
                            msg=msg)
            for i in SERVERS:
                msg = "server %s script %s" % (i,script,)
                print msg
                os.environ['PBS_DEFAULT'] = i
                self.assertTrue(self.compareResults(script, ignore=IGNORE),
                                msg=msg)
            for i in BAD_SERVERS:
                msg = "bad server %s script %s" % (i,script,)
                print msg
                os.environ['PBS_DEFAULT'] = i
                self.assertTrue(self.compareResults(script, ignore=IGNORE),
                                msg=msg)
    def testCombined(self):
        for s in SCRIPTS:
            for arg in GOOD_ARGS:
                msg = "good arg %s script %s"% (arg,s)
                print msg
                self.assertTrue(self.compareResults(s, arg),
                                msg=msg)

    def testSuffixes(self):
        for s in SCRIPTS:
            for arg in SUFFIXES_ARGS:
                msg = "suffx arg %s script %s"% (arg,s)
                print msg
                self.assertTrue(self.compareResults(s, arg),
                                msg=msg)
def suite():
    """ return all the tests"""
    return TestLoader().loadTestsFromTestCase(TestLegacySubmitfilter)

if __name__ == '__main__':
    """Use this __main__ block to help write and test unittests
        just uncomment the parts you need
    """
    main()


