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
All things moab.
"""
import xml.dom.minidom

from vsc.utils.fancylogger import getLogger
from vsc.utils.run import run_simple


logger = getLogger('vsc.jobs.moab')


class ShowqInfo(dict):
    """Code partially taken from http://stackoverflow.com/questions/6256183/combine-two-dictionaries-of-dictionaries-python."""

    def __init__(self, *args, **kwargs):
        super(ShowqInfo, self).__init__(*args, **kwargs)

    def add(self, user, host, state):

        if not user in self:
            self[user] = {}
        if not host in self[user]:
            self[user][host] = {}
        if not state in self[user][host]:
            self[user][host][state] = []

    def update(self, E=None, **F):
        if E is not None:
            if 'keys' in dir(E) and callable(getattr(E, 'keys')):
                for k in E:
                    if k in self:  # existing ...must recurse into both sides
                        self.r_update(k, E)
                    else:  # doesn't currently exist, just update
                        self[k] = E[k]
            else:
                for (k, v) in E:
                    self.r_update(k, {k: v})

        for k in F:
            self.r_update(k, {k: F[k]})

    def r_update(self, key, other_dict):

        if isinstance(self[key], dict) and isinstance(other_dict[key], dict):
            od = dict(self[key])
            nd = other_dict[key]
            od.update(nd)
            self[key] = od
        elif isinstance(self[key], list):
            if isinstance(other_dict[key], list):
                self[key] = self[key].extend(other_dict[key])
            else:
                self[key] = self[key].append(other_dict[key])
        else:
            self[key] = other_dict[key]


def process_attributes(job_xml, job, attributes):

    for attribute in attributes:
        job[attribute] = job_xml.getAttribute(attribute)
        if not job[attribute]:
            logger.error("Failed to find attribute name %s in %s" % (attribute, job_xml.toxml()))
            job.pop(attribute)


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
        res[user][host][state] = [job]

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

    options_ = []
    if xml:
        options_ += ['--xml']

    (exit_code, output) = run_simple([path] + options_)

    if exit_code != 0:
        return None

    if process:
        return parse_showq_xml(cluster, output)
    else:
        return output
