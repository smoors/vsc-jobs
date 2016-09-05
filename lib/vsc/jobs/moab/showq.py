#
# Copyright 2009-2016 Ghent University
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
from lxml import etree

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

    def _cache_pickle_name(self, host):
        """File name for the pickle file to cache results."""
        return ".showq.pickle.cluster_%s" % (host)

    def parser(self, host, txt):
        """
        Parse showq --xml output

        @type host: string
        @type txt: the real output provided by showq in XML format

        @param res: current dictionary woth the parsed outut for other hosts
        @param host: the name of the cluster we target

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
        xml = etree.fromstring(txt)

        self.logger.debug("Parsing showq output")

        for job in xml.findall('.//job'):
            user = job.attrib['User']
            state = job.attrib['State']

            self.logger.debug("Found job %s for user %s in state %s" % (job.attrib['JobID'], user, state))

            showq_info.add(user, host, state)

            j = {}
            j.update(self._process_attributes(job, mandatory_attributes))

            if state in('Running'):
                j.update(self._process_attributes(job, running_attributes))
            else:
                if 'BlockReason' in job.attrib:
                    if state in ('Idle'):
                        state = 'IdleBlocked'
                        showq_info.add(user, host, state)
                    j.update(self._process_attributes(job, blocked_attributes))
                else:
                    j.update(self._process_attributes(job, idle_attributes))

            # append the job
            showq_info[user][host][state] += [j]

        return showq_info


class SshShowq(Showq, SshMoabCommand):
    """
    Allows for retrieving showq information through an ssh command to the remote master
    """
    pass
