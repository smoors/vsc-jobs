# -*- coding: latin-1 -*-
#
# Copyright 2013-2018 Ghent University
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
All things checkjob.

@author Andy Georges
"""
import json
import pprint

from lxml import etree

from vsc.jobs.moab.internal import MoabCommand, SshMoabCommand
from vsc.utils.fancylogger import getLogger
from vsc.utils.missing import RUDict


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

        if user not in self:
            self[user] = RUDict()
        if host not in self[user]:
            self[user][host] = []

    def _display(self, job):
        """Show the data for a single job."""
        pass

    def display(self, jobid=None):
        """Yield a string representing the contents of the data for the given job id.

        If the job id is None, all results are given.
        """
        if not jobid:
            return pprint.pformat(self)

        location = [(user, host) for user in self for host in self[user] if jobid in self[user][host]]

        if not location:
            return ""

        if len(location) > 1:
            return None

        return pprint.pformat(self[location[0]][location[1]][jobid])


class Checkjob(MoabCommand):

    def __init__(self, clusters, cache_pickle=False, dry_run=False):

        MoabCommand.__init__(self, cache_pickle=cache_pickle, dry_run=dry_run)

        self.info = CheckjobInfo
        self.clusters = clusters

    def _cache_pickle_name(self, host):
        """File name for the pickle file to cache results."""
        return ".checkjob.pickle.cluster_%s" % (host)

    def _run_moab_command(self, commandlist, cluster, options):
        """Override the default, need to add an option"""
        options += ['-vvv', 'all']
        return super(Checkjob, self)._run_moab_command(commandlist, cluster, options)

    def parser(self, host, txt):
        """Parse the checkjob XML and produce a corresponding CheckjobInfo instance."""
        xml = etree.fromstring(txt, parser=etree.XMLParser(huge_tree=True))

        checkjob_info = CheckjobInfo()

        for job in xml.findall('.//job'):

            user = job.attrib['User']
            checkjob_info.add(user, host)
            checkjob_info[user][host] += [
                    (dict(job.attrib.items()), map(lambda r: dict(r.attrib.items()), job.getchildren()))
            ]

        return checkjob_info


class CheckjobInfoJSONEncoder(json.JSONEncoder):
    """Encoding for the CheckjobInfo class to a JSON format."""

    def default(self, obj):

        if isinstance(obj, CheckjobInfo):
            return obj.encode_json()
        else:
            return json.JSONEncoder.default(self, obj)


class SshCheckjob(Checkjob, SshMoabCommand):
    """
    Allows for retrieving checkjob information through an ssh command over a remote master
    """
    def __init__(self, master, user, clusters, cache_pickle=False, dry_run=False):
        SshMoabCommand.__init__(self, target_master=master, target_user=user, cache_pickle=cache_pickle, 
                dry_run=dry_run)
        Checkjob.__init__(self, clusters=clusters, cache_pickle=cache_pickle, dry_run=dry_run)
