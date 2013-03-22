#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2009-2013 Ghent University
#
# This file is part of vsc-jobs,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
##
"""
All things showq.

@author Andy Georges
"""
from lxml import etree

from vsc.jobs.moab.internal import moab_command, process_attributes

from vsc.utils.fancylogger import getLogger
from vsc.utils.missing import RUDict
from vsc.utils.run import RunAsyncLoop


logger = getLogger('vsc.jobs.moab.showq')


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

        if not user in self:
            self[user] = RUDict()
        if not host in self[user]:
            self[user][host] = RUDict()
        if not state in self[user][host]:
            self[user][host][state] = []


def parse_showq_xml(host, txt):
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
    StartPriority="660" StartTime="0" State="Idle" SubmissionTime="1278480000" SuspendDuration="0" User="vsc40000"></job>
    """
    mandatory_attributes = ['ReqProcs', 'SubmissionTime', 'JobID', 'DRMJID', 'Class']
    running_attributes = ['MasterHost']
    idle_attributes = []
    blocked_attributes = ['BlockReason', 'Description']

    doc = xml.dom.minidom.parseString(txt)

    res = ShowqInfo()

    for j in doc.getElementsByTagName("job"):
        job = {}
        user = j.getAttribute('User')
        state = j.getAttribute('State')

        logger.debug("Found job %s for user %s in state %s" % (j.getAttribute('JobID'), user, state))

        res.add(user, host, state)

        process_attributes(j, job, mandatory_attributes)

        if state in ('Running'):
            process_attributes(j, job, running_attributes)
        else:
            if j.hasAttribute('BlockReason'):

                if state == 'Idle':
                    ## redefine state
                    state = 'IdleBlocked'
                    res.add(user, host, state)
                process_attributes(j, job, blocked_attributes)

            else:
                process_attributes(j, job, idle_attributes)

        # append the job
        res[user][host][state] += [job]

    return res


def showq(path, cluster, options, xml=True, process=True):
    """Run the showq command and return the (processed) output.

    @type path: string
    @type options: list of strings
    @type xml: boolean
    @type process: boolean

    @param path: path to the showq executable
    @param options: The options to pass to the showq command.
    @param xml: Should we ask for output in xml format?
    @param process: Should we do postprocessing of the output here?
                    FIXME: the output format may depend on the options, so this may be fragile.

    @return: string if no processing is done, dict with the job information otherwise
    """

    return moab_command(path, cluster, options, parse_showq_xml, xml, process)
