#!/usr/bin/env python
#
# Copyright 2015-2016 Ghent University
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
Release jobholds
"""

import sys


from vsc.jobs.moab.internal import MoabCommand
from vsc.jobs.moab.showq import Showq
from vsc.utils.cache import FileCache
from vsc.utils import fancylogger
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

# Constants
NAGIOS_HEADER = "release_jobholds"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 60 * 60  # 60 minutes

RELEASEJOB_CACHE_FILE = '/var/cache/%s.json.gz' % NAGIOS_HEADER

RELEASEJOB_LIMITS = {
    # jobs in hold per user (maximum of all users)
    'peruser_warning': 10,
    'peruser_critical': 20,
    # total number of jobs in hold
    'total_warning': 50,
    'total_critical': 100,
    # per job release attempts (maximum of all jobs)
    'release_warning': 50,
    'release_critical': 70,
}

RELEASEJOB_SUPPORTED_HOLDTYPES = ('BatchHold',)

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()


def process_hold(clusters, dry_run=False):
    """Process a filtered queueinfo dict"""
    releasejob_cache = FileCache(RELEASEJOB_CACHE_FILE)

    # get the showq data
    for data in clusters.values():
        data['path'] = data['spath']  # showq path
    showq = Showq(clusters, cache_pickle=True)
    (queue_information, _, _) = showq.get_moab_command_information()

    # release the jobs, prepare the command
    m = MoabCommand(cache_pickle=False, dry_run=dry_run)
    for data in clusters.values():
        data['path'] = data['mpath']  # mjobctl path
    m.clusters = clusters

    # read the previous data
    ts_data = releasejob_cache.load('queue_information')
    if ts_data is None:
        old_queue_information = {}
    else:
        (_, old_queue_information) = ts_data

    stats = {
        'peruser': 0,
        'total': 0,
        'release': 0,
    }

    release_jobids = []

    for user, clusterdata in queue_information.items():
        oldclusterdata = old_queue_information.setdefault(user, {})
        totaluser = 0
        for cluster, data in clusterdata.items():
            olddata = oldclusterdata.setdefault(cluster, {})
            # DRMJID is supposed to be unique
            # get all oldjobids in one dict
            oldjobs = dict([(j['DRMJID'], j['_release']) for jt in olddata.values() for j in jt])
            for jobtype, jobs in data.items():
                removeids = []
                for idx, job in enumerate(jobs):
                    jid = job['DRMJID']

                    if jobtype in RELEASEJOB_SUPPORTED_HOLDTYPES:
                        totaluser += 1
                        release = max(oldjobs.get(jid, 0), 0) + 1
                        job['_release'] = release
                        stats['release'] = max(stats['release'], release)
                        release_jobids.append(jid)
                        # release the job
                        cmd = [m.clusters[cluster]['path'], '-u', jid]
                        logger.info("Releasing job %s cluster %s for the %s-th time." % (jid, cluster, release))
                        if dry_run:
                            logger.info("Dry run %s" % cmd)
                        else:
                            m._run_moab_command(cmd, cluster, [])
                    else:
                        # keep historical data, eg a previously released job could be idle now
                        # but keep the counter in case it gets held again
                        try:
                            release = oldjobs[jid]
                            job['_release'] = release
                        except KeyError:
                            # not previously in hold, remove it
                            removeids.append(idx)

                # remove the jobs (in reverse order)
                for remove_idx in removeids[::-1]:
                    jobs.pop(remove_idx)

                # cleanup
                if len(jobs) == 0:
                    data.pop(jobtype)
            # cleanup
            if len(data) == 0:
                clusterdata.pop(cluster)
        # cleanup
        if len(clusterdata) == 0:
            queue_information.pop(user)

        # update stats
        stats['peruser'] = max(stats['peruser'], totaluser)
        stats['total'] += totaluser

    logger.info("Release statistics: total jobs in hold %(total)s; max in hold per user %(peruser)s; max releases per job %(release)s" % stats)

    # update and close
    releasejob_cache.update('queue_information', queue_information, 0)
    releasejob_cache.close()

    return release_jobids, stats


def main():
    """Main function"""
    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'hosts': ('the hosts/clusters that should be contacted for job information', None, 'extend', []),
        'cache': ('the location to store the cache with previous release hold data', None, 'store',
                  RELEASEJOB_CACHE_FILE)
    }

    opts = ExtendedSimpleOption(options)

    try:
        # parse config file
        clusters = {}
        for host in opts.options.hosts:
            master = opts.configfile_parser.get(host, "master")
            showq_path = opts.configfile_parser.get(host, "showq_path")
            mjobctl_path = opts.configfile_parser.get(host, "mjobctl_path")
            clusters[host] = {
                'master': master,
                'spath': showq_path,
                'mpath': mjobctl_path,
            }

        # process the new and previous data
        released_jobids, stats = process_hold(clusters, dry_run=opts.options.dry_run)
    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    # nagios state
    stats.update(RELEASEJOB_LIMITS)
    stats['message'] = "released %s jobs in hold" % len(released_jobids)
    opts.epilogue("Release jobholds finished", stats)


if __name__ == '__main__':
    main()
