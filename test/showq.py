#
# Copyright 2016-2017 Ghent University
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


SHOWQ_ONE_JOB = """
<Data><queue count="375" option="active">
<job Account="gvo00002"
BlockReason="IdlePolicy:job 1109247 violates idle HARD MAXIPROC limit of 3528 for user vsc40485  partition ALL (Req: 480  InUse: 3192)"
Class="short" DRMJID="1109247.master19.golett.gent.vsc" EEDuration="337477" GJID="1109247" Group="vsc40485" JobID="1109247"
JobName="JUBE-wrf-conus_2.5" PAL="golett" ReqAWDuration="14400" ReqProcs="480" StartPriority="1751" StartTime="0"
State="Idle" SubmissionTime="1507045378" SuspendDuration="0" User="vsc40485"/>
</queue></Data>
"""

JOBCTL_ONE_JOB = """
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
</Data>
"""

class TestSshShowq(TestCase):
    def test_parser(self):
        """Test the showq parsers"""
        sq = Showq('clusters')
        master = 'master19.golett.gent.vsc'
        showq = sq.parser(master, SHOWQ_ONE_JOB)
        sq.jobctl = True
        jobctl = sq.parser(master, JOBCTL_ONE_JOB)
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
