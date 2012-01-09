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

SCRIPT1="""#!/bin/sh
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
"""
SCRIPT5="""#!/bin/sh
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
"""
SCRIPT6="""#!/bin/sh
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
SCRIPT7="""#!/bin/sh
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
"""
SCRIPT4="""#!/bin/sh
#PBS -l walltime=11:25:00
#PBS -m bea
#
cd $VSC_HOME 
"""
SCRIPT2="""#!/bin/sh
#PBS -l walltime=11:25:00 
#PBS -l vmem=500mb
#PBS -q short
#

cd $VSC_HOME
##logs to stderr by default, redirect this to stdout
./pfgw64s 42424242_1t.txt 2>> $VSC_SCRATCH/testrun.42424242.out 

#PBS -m bea
"""
SCRIPT3="""#!/bin/sh
#PBS -l walltime=11:25:00 
##PBS -l vmem=500mb
#PBS -q short
#PBS -m bea
#"""
GOOD_ARGS=['','-m bea','-l vmem=1000mb','-l  nodes=1:ppn=7','-q short','-q  d short',
      '-q short@gengar', '-q @blashort', '-q @blashort@gengar','-x ignorethis',
      '--ignorethis','--ignorethis da','-x','-h x']
BAD_ARGS=['-m','-l'] #this should not occur, qsub checks this


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
    return out,err

class TestSubmitfilter(unittest.TestCase):
    

    
    def compareResults(self,input,args=""):
        old = runcmd("""echo '%s' | ../files/submitfilter %s""" % (input,args))
        new = runcmd("""echo '%s' | ../files/submitfilter.py %s""" % (input,args))
        pprint (list(difflib.Differ().compare(old[0].splitlines(1),new[0].splitlines(1))))
        old = old[0].replace('\n','')
        new = new[0].replace('\n','')
        for i in old:
            if not i in new:
                return False
        for i in new:
            if not i in old:
                return False
        return True    
    
    def testCLIOptions(self):
        for arg in GOOD_ARGS:
            print arg
            self.assertTrue(self.compareResults("",arg))
    
    def testempty(self):
        self.assertTrue(self.compareResults(""))
        
    def testscript1(self):
        self.assertTrue(self.compareResults(SCRIPT1))
    def testscript5(self):
        self.assertTrue(self.compareResults(SCRIPT5))
    def testscript6(self):
        self.assertTrue(self.compareResults(SCRIPT6))
    def testscript7(self):
        self.assertTrue(self.compareResults(SCRIPT7))
        
    def testscript3(self):
        self.assertTrue(self.compareResults(SCRIPT3))
        pass
    
    def testscript2(self):
        #fails because in new we check for the end of the preamble 
        #self.assertTrue(self.compareResults(SCRIPT2))
        pass
    
    def testCombined(self):
        for s in [SCRIPT1]:
            for arg in GOOD_ARGS:
                print arg
                self.assertTrue(self.compareResults(s,arg))
                
                

print runcmd("""echo "hello\n" | ../files/submitfilter.py -l vmem=1000mb """)
