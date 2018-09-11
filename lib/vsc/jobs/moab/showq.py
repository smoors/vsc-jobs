#
# Copyright 2009-2018 Ghent University
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
All things showq.

@author Andy Georges
"""
import os
from lxml.etree import XMLParser, fromstring

from vsc.jobs.moab.internal import MoabCommand, SshMoabCommand
from vsc.utils.missing import RUDict


class ShowqInfo(RUDict):
    """Dictionary to keep track of queued jobs.

    Basic key structure is
        - user
            - host
                - state (Running, Idle, Blocked, ...)
    """

    def __init__(self, *args, **kwargs):
        super(ShowqInfo, self).__init__(*args, **kwargs)

    def add(self, user, host, state):

        if user not in self:
            self[user] = RUDict()
        if host not in self[user]:
            self[user][host] = RUDict()
        if state not in self[user][host]:
            self[user][host][state] = []


class Showq(MoabCommand):
    """Run showq and gather the results."""

    def __init__(self, clusters, cache_pickle=False, dry_run=False):

        super(Showq, self).__init__(cache_pickle, dry_run)

        self.info = ShowqInfo
        self.clusters = clusters
        self.jobctl = False

    def _cache_pickle_name(self, host):
        """File name for the pickle file to cache results."""
        return ".showq.pickle.cluster_%s" % (host)

    def _command(self, path):
        """Retreive queue info from jobctl ALL"""
        jobctl = os.path.join(os.path.dirname(path), 'mjobctl')
        if os.path.exists(jobctl):
            self.jobctl = True
            return [jobctl, '-q', 'diag', 'ALL']
        else:
            self.logger.warning("No jobctl command found, using showq path %s", path)
            return super(Showq, self)._command(path)

    def parser(self, host, txt):
        """
        Parse showq or jobctl XML output

        @type host: the name of the cluster we target
        @type txt: the real output provided by showq in XML format

        @returns res: updated dictionary with the showq information for this host.

        <job AWDuration="3931" Account="gvo00000" Class="short" DRMJID="123456788.master.gengar.gent.vsc"
        EEDuration="1278479828" Group="vsc40000" JobID="123456788" JobName="job.sh" MasterHost="node129"
        PAL="gengar" ReqAWDuration="7200" ReqProcs="8" RsvStartTime="1278480000" RunPriority="663"
        StartPriority="663" StartTime="127848000" StatPSDed="31467.120000" StatPSUtl="3404.405600"
        State="Running" SubmissionTime="1278470000" SuspendDuration="0" User="vsc40000">
        <job Account="gvo00000" BlockReason="IdlePolicy" Class="short" DRMJID="1231456789.master.gengar.gent.vsc"
        Description="job 123456789 violates idle HARD MAXIPROC limit of 800 for user vsc40000  (Req: 8  InUse: 800)"
        EEDuration="1278486173" Group="vsc40023" JobID="1859934" JobName="job.sh" ReqAWDuration="7200" ReqProcs="8"
        StartPriority="660" StartTime="0" State="Idle" SubmissionTime="1278480000" SuspendDuration="0" User="vsc40000">
        </job>
        """
        mandatory_attributes = ['ReqProcs', 'SubmissionTime', 'JobID', 'DRMJID', 'Class']
        running_attributes = ['MasterHost']
        idle_attributes = []
        blocked_attributes = ['BlockReason', 'Description']

        showq_info = ShowqInfo()
        p = XMLParser(huge_tree=True)
        xml = fromstring(txt, parser=p)

        self.logger.debug("Parsing showq output")

        for job in xml.findall('.//job'):
            if self.jobctl:
                # updates the job in place
                if not self._process_jobctl(job):
                    continue

            user = job.attrib['User']
            state = job.attrib['State']

            self.logger.debug("Found job %s for user %s in state %s" % (job.attrib['JobID'], user, state))

            showq_info.add(user, host, state)

            j = {}

            j.update(self._process_attributes(job, mandatory_attributes))

            if state in('Running',):
                j.update(self._process_attributes(job, running_attributes))
            else:
                if 'BlockReason' in job.attrib:
                    if state in ('Idle',):
                        state = 'IdleBlocked'
                        showq_info.add(user, host, state)
                    j.update(self._process_attributes(job, blocked_attributes))
                else:
                    j.update(self._process_attributes(job, idle_attributes))

            # append the job
            showq_info[user][host][state] += [j]

        return showq_info

    def _process_jobctl(self, job):
        """
        Adapt parsed mjobctl -q diag ALL --xml output to match parsed showq XML structure

        Update job lxml etree element in place.
        Retruns False if the whole job can be ignored (e.g. in case of an internal array parentjob)

        # invalid xml due to forced line breaks
        <job Account="gvo00002" BecameEligible="1508512430"
        BlockReason="IdlePolicy:job 1109247 violates idle HARD MAXIPROC limit of 3528 for user vsc40485  partition ALL \
            (Req: 480  InUse: 3192)"
        Bypass="5" Class="short" DRMJID="1109247.master19.golett.gent.vsc" EEDuration="337477"
        EFile="gligar03.gligar.gent.vsc:/user/data/gent/vsc404/vsc40485/jube/JUBE-benchmarks/JUBE_jobs_output/delcatty/$PBS_JOBID.err"
        EState="Idle" EffPAL="[golett]" Flags="RESTARTABLE" GAttr="checkpoint" GJID="1109247" Group="vsc40485"
        HostList="node2551.golett.gent.vsc:24,node2552.golett.gent.vsc:24,node2553.golett.gent.vsc:24,node2554.golett.gent.vsc:24,\
            node2555.golett.gent.vsc:24,node2556.golett.gent.vsc:24,node2557.golett.gent.vsc:24,node2558.golett.gent.vsc:24,\
            node2559.golett.gent.vsc:24,node2560.golett.gent.vsc:24,node2561.golett.gent.vsc:24,node2562.golett.gent.vsc:24,\
            node2563.golett.gent.vsc:24,node2564.golett.gent.vsc:24,node2565.golett.gent.vsc:24,node2566.golett.gent.vsc:24,\
            node2567.golett.gent.vsc:24,node2568.golett.gent.vsc:24,node2569.golett.gent.vsc:24,node2570.golett.gent.vsc:24"
        IWD="/kyukon/home/gent/vsc404/vsc40485" JobID="1109247" JobName="JUBE-wrf-conus_2.5" JobRank="0"
        OFile="gligar03.gligar.gent.vsc:/user/data/gent/vsc404/vsc40485/jube/JUBE-benchmarks/JUBE_jobs_output/delcatty/$PBS_JOBID.out"
        PAL="DEFAULT,SHARED,golett" QueueStatus="blocked" RM="golett"
        RMStdErr="gligar03.gligar.gent.vsc:/user/data/gent/vsc404/vsc40485/jube/JUBE-benchmarks/JUBE_jobs_output/delcatty/$PBS_JOBID.err"
        RMStdOut="gligar03.gligar.gent.vsc:/user/data/gent/vsc404/vsc40485/jube/JUBE-benchmarks/JUBE_jobs_output/delcatty/$PBS_JOBID.out"
        ReqAWDuration="14400" SRMJID="1109247.master19.golett.gent.vsc" StartPriority="1751" StatMSUtl="0.000"
        StatPSUtl="0.000" State="Idle" SubmissionTime="1507045378" SuspendDuration="0" TemplateSetList="DEFAULT"
        User="vsc40485">
        <req ReqNodeMem="&gt;=0" ReqNodeProc="&gt;=0" ReqNodeSwap="&gt;=0" ReqProcPerTask="1" ReqSwapPerTask="149"
        TCReqMin="480" TPN="24"/>
        <tx/>
        </job>

        showq data for same job:
        <job Account="gvo00002"
        BlockReason="IdlePolicy:job 1109247 violates idle HARD MAXIPROC limit of 3528 for user vsc40485  partition ALL \
            (Req: 480  InUse: 3192)"
        Class="short" DRMJID="1109247.master19.golett.gent.vsc" EEDuration="337477" GJID="1109247" Group="vsc40485"
        JobID="1109247" JobName="JUBE-wrf-conus_2.5" PAL="golett" ReqAWDuration="14400" ReqProcs="480"
        StartPriority="1751" StartTime="0" State="Idle" SubmissionTime="1507045378" SuspendDuration="0"
        User="vsc40485"/>

        Array parent job:
        <job ... JobID="2513782" --> no [] or ()
        ... RM="internal" ... >
        <req ... "/><ArrayInfo Active="9" Complete="0" Count="9" Idle="0" Name="2513782">
        <child JobArrayIndex="1" Name="2513782[1]" State="Running"/>
        ...
        <child JobArrayIndex="9" Name="2513782[9]" State="Running"/></ArrayInfo>
        <tx/></job>
        """

        if job.get('RM') == 'internal':
            for child in job:
                if child.tag == 'ArrayInfo':
                    # Parent arrayjob
                    return False

        def set_attr(new, old, subtree, func):
            if new not in job.attrib:
                for child in job:
                    if child.tag == subtree:
                        val = child.get(old)
                        if val is not None:
                            job.set(new, func(val))

        # handle jobctl xml ReqProcs via req child
        set_attr('ReqProcs', 'TCReqMin', 'req', lambda x: x)

        # MasterHost
        # format, eg: AllocNodeList="node2535.golett.gent.vsc:24,node2536.golett.gent.vsc:24"
        set_attr('MasterHost', 'AllocNodeList', 'req', lambda x: x.split(",")[0].split(":")[0])

        # Hold state
        if 'Hold' in job.attrib:
            job.set('State', "%sHold" % job.get('Hold'))

        # TODO: fix missing Description errors
        #if 'BlockReason' in job.attrib:
        #    reason = job.get('BlockReason').split(':', 1)
        #    if 'Description' not in job.attrib and len(reason) == 2:
        #        job.set('BlockReason', reason[0])
        #        job.set('Description', reason[1])

        # Keep this job
        return True

class SshShowq(Showq, SshMoabCommand):
    """
    Allows for retrieving showq information through an ssh command to the remote master
    """
    def __init__(self, target_master, target_user, clusters, cache_pickle=False, dry_run=False):
        Showq.__init__(self, clusters=clusters, cache_pickle=cache_pickle, dry_run=dry_run)
        SshMoabCommand.__init__(self, target_master=target_master, target_user=target_user, cache_pickle=cache_pickle,
                dry_run=dry_run)
