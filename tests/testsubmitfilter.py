"""
@author: Jens Timmerman
"""

import unittest
from subprocess import Popen, PIPE
import time
import os
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
           'master11.dugtrio.gent.vsc',
           'master13.raichu.gent.vsc',
           'master-moab.muk.os',
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


def runcmd(cmd):
    start = datetime.datetime.now()
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, close_fds=True)
    timedout = False
    while p.poll() is None:
        time.sleep(1)
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
#        print "-"*9 + "\n" + out
#        print "cmd %s, out: %s, err: %s "%(cmd,out,err)
    return out, err


class TestSubmitfilter(unittest.TestCase):
    def compareResults(self, input, args="", ignore=""):
        old = runcmd("""echo '%s' | python ./testsubmitfilter/submitfilter.py %s""" % (input, args))
        new = runcmd("""echo '%s' | python ../bin/submitfilter.py %s""" % (input, args))
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
            print arg
            self.assertTrue(self.compareResults("", arg))

    def testempty(self):
        self.assertTrue(self.compareResults(""))

    def testscripts(self):
        os.environ['PBS_DEFAULT'] = "zever"
        for script in SCRIPTS:
            del os.environ['PBS_DEFAULT']
            self.assertTrue(self.compareResults(script))
            for i in SERVERS:
                print i
                os.environ['PBS_DEFAULT'] = i
                self.assertTrue(self.compareResults(script))
            for i in BAD_SERVERS:
                print i
                os.environ['PBS_DEFAULT'] = i
                self.assertTrue(self.compareResults(script))

    def testscipts2(self):
        """
        compares old submitfilter with new one
        but ignores certain bugs in the old script
        """
        os.environ['PBS_DEFAULT'] = "zever"
        for script in SCRIPTS2:
            del os.environ['PBS_DEFAULT']
            self.assertTrue(self.compareResults(script, ignore=IGNORE))
            for i in SERVERS:
                print i
                os.environ['PBS_DEFAULT'] = i
                self.assertTrue(self.compareResults(script, ignore=IGNORE))
            for i in BAD_SERVERS:
                print i
                os.environ['PBS_DEFAULT'] = i
                self.assertTrue(self.compareResults(script, ignore=IGNORE))

    def testCombined(self):
        for s in SCRIPTS:
            for arg in GOOD_ARGS:
                print arg
                self.assertTrue(self.compareResults(s, arg))

    def testSuffixes(self):
        for s in SCRIPTS:
            for arg in SUFFIXES_ARGS:
                print arg
                self.assertTrue(self.compareResults(s, arg))


print runcmd("""echo "hello\n" | ../bin/submitfilter.py -l vmem=1000mb """)
