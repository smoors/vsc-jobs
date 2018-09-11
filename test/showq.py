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
@author: stdweird
"""
from mock import patch

import os
import sys
from vsc.install.testing import TestCase

from vsc.jobs.moab.showq import Showq, SshShowq, ShowqInfo


SHOWQ_JOBS = """
<Data><queue count="375" option="active">
<job Account="gvo00002"
BlockReason="IdlePolicy:job 1109247 violates idle HARD MAXIPROC limit of 3528 for user vsc40485  partition ALL (Req: 480  InUse: 3192)"
Class="short" DRMJID="1109247.master19.golett.gent.vsc" EEDuration="337477" GJID="1109247" Group="vsc40485" JobID="1109247"
JobName="JUBE-wrf-conus_2.5" PAL="golett" ReqAWDuration="14400" ReqProcs="480" StartPriority="1751" StartTime="0"
State="Idle" SubmissionTime="1507045378" SuspendDuration="0" User="vsc40485"/>
<job AWDuration="238262" Account="gvo00028" Class="long" DRMJID="2513269.master19.golett.gent.vsc" EEDuration="55" GJID="2513269" Group="vsc40002" JobID="2513269" JobName="myname" MasterHost="node832.golett.gent.vsc" PAL="golett" ReqAWDuration="259200" ReqProcs="1" RsvStartTime="1510331373" RunPriority="-406" StartPriority="-406" StartTime="1510331373" StatPSDed="238258.370000" StatPSUtl="240785.929800" State="Running" SubmissionTime="1510331318" SuspendDuration="0" User="vsc40002"/>
<job Account="gvo00002" Class="short" DRMJID="12345.master19.golett.gent.vsc" EEDuration="4150" GJID="12345" Group="vsc12345" JobID="12345" JobName="jobG_4778" ReqAWDuration="41399" ReqProcs="6" StartPriority="323" StartTime="0" State="BatchHold" SubmissionTime="1511065918" SuspendDuration="0" User="vsc12345"/>
</queue></Data>
"""

JOBCTL_JOBS = """
<Data>
<job Account="gvo00002" BecameEligible="1508512430"
BlockReason="IdlePolicy:job 1109247 violates idle HARD MAXIPROC limit of 3528 for user vsc40485  partition ALL (Req: 480  InUse: 3192)"
Bypass="5" Class="short" DRMJID="1109247.master19.golett.gent.vsc" EEDuration="337477"
EFile="gligar03.gligar.gent.vsc:/user/data/gent/vsc404/vsc40485/jube/JUBE-benchmarks/JUBE_jobs_output/delcatty/$PBS_JOBID.err"
EState="Idle" EffPAL="[golett]" Flags="RESTARTABLE" GAttr="checkpoint" GJID="1109247" Group="vsc40485"
HostList="node2551.golett.gent.vsc:24,node2552.golett.gent.vsc:24,node2553.golett.gent.vsc:24,node2554.golett.gent.vsc:24,node2555.golett.gent.vsc:24,node2556.golett.gent.vsc:24,node2557.golett.gent.vsc:24,node2558.golett.gent.vsc:24,node2559.golett.gent.vsc:24,node2560.golett.gent.vsc:24,node2561.golett.gent.vsc:24,node2562.golett.gent.vsc:24,node2563.golett.gent.vsc:24,node2564.golett.gent.vsc:24,node2565.golett.gent.vsc:24,node2566.golett.gent.vsc:24,node2567.golett.gent.vsc:24,node2568.golett.gent.vsc:24,node2569.golett.gent.vsc:24,node2570.golett.gent.vsc:24"
IWD="/kyukon/home/gent/vsc404/vsc40485" JobID="1109247" JobName="JUBE-wrf-conus_2.5" JobRank="0"
OFile="gligar03.gligar.gent.vsc:/user/data/gent/vsc404/vsc40485/jube/JUBE-benchmarks/JUBE_jobs_output/delcatty/$PBS_JOBID.out"
PAL="DEFAULT,SHARED,golett" QueueStatus="blocked" RM="golett"
RMStdErr="gligar03.gligar.gent.vsc:/user/data/gent/vsc404/vsc40485/jube/JUBE-benchmarks/JUBE_jobs_output/delcatty/$PBS_JOBID.err"
RMStdOut="gligar03.gligar.gent.vsc:/user/data/gent/vsc404/vsc40485/jube/JUBE-benchmarks/JUBE_jobs_output/delcatty/$PBS_JOBID.out"
ReqAWDuration="14400" SRMJID="1109247.master19.golett.gent.vsc" StartPriority="1751" StatMSUtl="0.000" StatPSUtl="0.000"
State="Idle" SubmissionTime="1507045378" SuspendDuration="0" TemplateSetList="DEFAULT" User="vsc40485">
<req ReqNodeMem="&gt;=0" ReqNodeProc="&gt;=0" ReqNodeSwap="&gt;=0" ReqProcPerTask="1" ReqSwapPerTask="149" TCReqMin="480" TPN="24"/>
<tx/>
</job>
<job AWDuration="238262" Account="gvo00002" Class="long" DRMJID="2513269.master19.golett.gent.vsc" EEDuration="55"
EFile="gligar02.gligar.gent.vsc:/kyukon/home/gent/vsc400/vsc40002/thesis/BasketballDrive_1920x1080_50/c100/myname.e2513269"
EState="Running" EffPAL="[golett]" Flags="RESTARTABLE,FSVIOLATION" GAttr="FSVIOLATION,checkpoint" GJID="2513269" Group="vsc40002"
IWD="/kyukon/home/gent/vsc400/vsc40002/thesis/abc/c100" JobID="2513269"
JobName="myname" JobRank="0"
OFile="gligar02.gligar.gent.vsc:/kyukon/home/gent/vsc400/vsc40002/thesis/abc/c100/myname.o2513269"
PAL="DEFAULT,SHARED,golett" QueueStatus="active" RM="golett"
RMStdErr="gligar02.gligar.gent.vsc:/kyukon/home/gent/vsc400/vsc40002/thesis/abc/c100/myname.e2513269"
RMStdOut="gligar02.gligar.gent.vsc:/kyukon/home/gent/vsc400/vsc40002/thesis/abc/c100/myname.o2513269"
ReqAWDuration="259200" SRMJID="2513269.master19.golett.gent.vsc" SessionID="58722" StartCount="1" StartPriority="-406"
StartTime="1510331373" StatMSUtl="618489537.740" StatPSDed="238258.370" StatPSUtl="240785.930" State="Running"
SubmissionTime="1510331318" SuspendDuration="0" TemplateSetList="DEFAULT" User="vsc40002">
<req AllocNodeList="node832.golett.gent.vsc" AllocPartition="golett" NCReqMin="1" ReqNodeMem="&gt;=0" ReqNodeProc="&gt;=0"
ReqNodeSwap="&gt;=0" ReqPartition="golett" ReqProcPerTask="1" ReqSwapPerTask="2610" TCReqMin="1" UtilMem="2610" UtilProc="1" UtilSwap="1684"/>
<tx/></job>
<job Account="gvo00002" BecameEligible="1511065949" Bypass="45" Class="short" DRMJID="12345.master19.golett.gent.vsc" EEDuration="4150"
EFile="gligar01:/user/home/gent/vsc123/vsc12345/blah.gausserr" EState="Idle" EffPAL="[golett]"
Flags="RESTARTABLE,FSVIOLATION" GAttr="FSVIOLATION,checkpoint" GJID="12345" Group="vsc12345" Hold="Batch"
IWD="/kyukon/scratch/gent/vsc123/vsc12345" JobID="12345" JobName="jobG_4778" JobRank="0"
OFile="gligar01.gligar.gent.vsc:/user/home/gent/vsc123/vsc12345/blah.gaussout" PAL="DEFAULT,SHARED,golett"
QueueStatus="blocked" RM="golett"
RMStdErr="gligar01.gligar.gent.vsc:/user/home/gent/vsc123/vsc12345/blah.gausserr"
RMStdOut="gligar01.gligar.gent.vsc:/user/home/gent/vsc123/vsc12345/blah.gaussout"
ReqAWDuration="41399" SRMJID="12345.master19.golett.gent.vsc" StartCount="199" StartPriority="323" StatMSUtl="0.000"
StatPSUtl="0.000" State="Idle" SubmissionTime="1511065918" SuspendDuration="0" TemplateSetList="DEFAULT" User="vsc12345">
<req ReqNodeMem="&gt;=0" ReqNodeProc="&gt;=0" ReqNodeSwap="&gt;=0" ReqPartition="golett" ReqProcPerTask="1"
ReqSwapPerTask="2996" TCReqMin="6" TPN="6"/>
<Messages><message COUNT="2" CTIME="1511367277" DATA="cannot modify job - cannot set job '12345.master19.golett.gent.vsc' attr 'hold:' to 'NULL' (rc: 15157 'The requested state or substate can't be set from the job's current state.') " EXPIRETIME="1511453677"
PRIORITY="0" TYPE="[NONE]" index="0"/><message COUNT="10" CTIME="1511369237"
DATA="cannot start job 12345 - RM failure, rc: 15046, msg: 'Resource temporarily unavailable'" EXPIRETIME="1511455634"
PRIORITY="0" TYPE="hold" index="1"/></Messages><tx/></job>
</Data>
"""

class TestSshShowq(TestCase):
    def test_parser(self):
        """Test the showq parsers"""
        sq = Showq('clusters')
        master = 'master19.golett.gent.vsc'
        showq = sq.parser(master, SHOWQ_JOBS)
        sq.jobctl = True
        jobctl = sq.parser(master, JOBCTL_JOBS)
        self.assertEqual(showq, jobctl, msg='showq and jobctl commands give same parsed result')

    def test_sshshowq(self):
        """Test sshshowq"""


        clusters = {'delcatty': {'path': '/opt/moab/bin/checkjob', 'master': 'master15.delcatty.gent.vsc'}, 'phanpy': {'path': '/opt/moab/bin/checkjob', 'master': 'master17.phanpy.gent.vsc'}, 'raichu': {'path': '/opt/moab/bin/checkjob', 'master': 'master13.raichu.gent.vsc'}, 'golett': {'path': '/opt/moab/bin/checkjob', 'master': 'master19.golett.gent.vsc'}, 'swalot': {'path': '/opt/moab/bin/checkjob', 'master': 'master21.swalot.gent.vsc'}}

        showq = SshShowq(
            'master1',
            'testuser',
            clusters=clusters,
            cache_pickle=True,
            dry_run=True)
        self.assertEqual(showq._command('/opt/moab/bin/checkjob'), ['sudo', 'ssh', 'testuser@master1', '/opt/moab/bin/checkjob'])
        self.assertEquals(showq.info, ShowqInfo)
        self.assertEquals(showq.info(), {})
