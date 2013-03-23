#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2013-2013 Ghent University
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
All things checkjob.

@author Andy Georges
"""

from lxml import etree

from vsc.jobs.moab.internal import MoabCommand
from vsc.utils.fancylogger import getLogger
from vsc.utils.missing import RUDict
from vsc.utils.run import RunAsyncLoop


logger = getLogger('vsc.jobs.moab.checkjob')


class CheckjobInfo(RUDict):
    """Dictionary to keep track of checkjob information.

    Basic structure is
        - user
            - host
                - jobinformation
    """

    def __init__(self, *args, **kwargs):
        super(CheckjobInfo, self).__init__(*args, **kwargs)

    def add(self, user, host):

        if not user in self:
            self[user] = RUDict()
        if not host in self[user]:
            self[user][host] = []


class Checkjob(MoabCommand):

    def __init__(self, clusters, dry_run):

        super(Checkjob, self).__init__(dry_run)

        self.info = CheckjobInfo
        self.clusters = clusters

    def _cache_pickle_name(self, host):
        """File name for the pickle file to cache results."""
        return ".checkjob.pickle.cluster_%s" % (host)

    def parser(self, host, txt):
        """Parse the checkjob XML and produce a corresponding CheckjobInfo instance."""

        xml = etree.fromstring(txt)

        checkjob_info = CheckjobInfo()

        for job in xml.findall('job'):

            user = job.attrib['User']
            checkjob_info.add(user, host)
            checkjob_info[user][host] += [(job.attrib, map(lambda r: r.attrib, job.getChildren()))]

        return checkjob_info
