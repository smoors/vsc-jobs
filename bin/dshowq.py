#!/usr/bin/env python-noenv
#
# Copyright 2009-2017 Ghent University
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
- The dshowq scripts collects showq information from all Tier-2 clusters and distributes it
in the user's home directory to allow faster lookup. On the Tier-1 machine it stores the
showq pickle files in the users personal fileset.

@author Stijn De Weirdt
@author Andy Georges

It should run on a regular bass to avoid information to become (too) outdated.
"""

import sys
import time


from vsc.administration.user import cluster_user_pickle_store_map, cluster_user_pickle_location_map
from vsc.accountpage.client import AccountpageClient
from vsc.config.base import VscStorage
from vsc.filesystem.gpfs import GpfsOperations
from vsc.jobs.moab.showq import SshShowq
from vsc.utils import fancylogger
from vsc.utils.fs_store import store_on_gpfs
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

# Constants
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

DEFAULT_VO = 'gvo00012'

STORE_LIMIT_CRITICAL = 5
INACTIVE = 'inactive'

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()


def collect_vo_account_page(active_users, rest_client):
    """
    Determines a mapping between each active user and the fellow members of his VO.

    If the user belongs to a non-default VO, then the map will have a value that is a dict
    of all other active users from the VO. Otherwise, it will have a singleton value. For
    compatibility purposes, the values map user names to an empty string (previously to gecos).
    """

    all_vos = rest_client.vo.get()
    vo_information = [rest_client.vo[vo[0]].get() for vo in all_vos if vo[1] not in (INACTIVE,)]
    user_to_vo_map = dict((u, vo) for vo in vo_information for u in vo['members'])

    user_maps_per_vo = {}
    found = set()
    for user in active_users:

        if user in found:
            continue

        vo = user_to_vo_map.get(user, None)
        if vo:
            if vo['vsc_id'] in (DEFAULT_VO,):
                logger.debug('user %s belongs to the default VO %s', user, vo['vsc_id'])
                found.add(user)
                user_maps_per_vo[user] = {user: ""}
            else:
                user_map = dict((u, "") for u in vo['members'] if u in active_users)
                for u in user_map:
                    found.add(u)

                user_maps_per_vo[vo['vsc_id']] = user_map

    return (found, user_maps_per_vo)


def determine_target_information(information, active_users, queue_information, rest_client):
    """Determine for the given information type, what should be stored for which users."""

    logger.debug("Determining target information for %s" % (information,))

    if information == 'user':
        user_info = dict([(u, {u: ""}) for u in active_users])  # FIXME: faking it
        return (active_users, dict([(user, {user: queue_information[user]}) for user in active_users]), user_info)
    elif information == 'vo':
        (all_target_users, user_maps_per_vo) = collect_vo_account_page(active_users, rest_client)

        target_queue_information = {}
        for vo in user_maps_per_vo.values():
            filtered_queue_information = dict([(user_id, queue_information[user_id]) for user_id in vo if user_id in queue_information])
            target_queue_information.update(dict([(user_id, filtered_queue_information) for user_id in vo]))

        return (all_target_users, target_queue_information, user_maps_per_vo)
    elif information == 'project':
        return (None, None, None)


def get_pickle_path(location, user_id, rest_client):
    """Determine the path (directory) where the pickle file with the queue information should be stored.

    @type location: string
    @type user_id: string

    @param location: indication of the user accessible storage spot to use, e.g., home or scratch
    @param user_id: VSC user ID
    @param rest_client: VscAccountpageClient instance

    @returns: tuple of (string representing the directory where the pickle file should be stored,
                        the relevant storing function in vsc.utils.fs_store).
    """
    return cluster_user_pickle_location_map[location](user_id, rest_client=rest_client).pickle_path()


def main():
    # Collect all info

    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'hosts': ('the hosts/clusters that should be contacted for job information', None, 'extend', []),
        'information': ('the sort of information to store: user, vo, project', None, 'store', 'user'),
        'location': ('the location for storing the pickle file: delcatty, muk', str, 'store', 'delcatty'),
        'account_page_url': ('the URL at which the account page resides', None, 'store', None),
        'access_token': ('the token that will allow authentication against the account page', None, 'store', None),
        'target_master': ('the master used to execute showq commands', None, 'store', None),
        'target_user': ('the user for ssh to the target master', None, 'store', None),
    }

    opts = ExtendedSimpleOption(options)

    try:
        rest_client = AccountpageClient(token=opts.options.access_token)

        gpfs = GpfsOperations()
        storage = VscStorage()
        storage_name = cluster_user_pickle_store_map[opts.options.location]
        login_mount_point = storage[storage_name].login_mount_point
        gpfs_mount_point = storage[storage_name].gpfs_mount_point

        clusters = {}
        for host in opts.options.hosts:
            master = opts.configfile_parser.get(host, "master")
            showq_path = opts.configfile_parser.get(host, "showq_path")
            clusters[host] = {
                'master': master,
                'path': showq_path
            }

        logger.debug("clusters = %s" % (clusters,))
        showq = SshShowq(opts.options.target_master,
                               opts.options.target_user,
                               clusters,
                               cache_pickle=True,
                               dry_run=opts.options.dry_run)

        logger.debug("Getting showq information ...")

        (queue_information, _, _) = showq.get_moab_command_information()
        timeinfo = time.time()

        active_users = queue_information.keys()

        logger.debug("Active users: %s" % (active_users))
        logger.debug("Queue information: %s" % (queue_information))

        # We need to determine which users should get an updated pickle. This depends on
        # - the active user set
        # - the information we want to provide on the cluster(set) where this script runs
        # At the same time, we need to determine the job information each user gets to see
        tup = (opts.options.information, active_users, queue_information, rest_client)
        (target_users, target_queue_information, user_map) = determine_target_information(*tup)

        nagios_user_count = 0
        nagios_no_store = 0

        stats = {}

        for user in target_users:
            try:
                path = get_pickle_path(opts.options.location, user, rest_client)
                user_queue_information = target_queue_information[user]
                user_queue_information['timeinfo'] = timeinfo
                store_on_gpfs(user, path, "showq", (user_queue_information, user_map[user]), gpfs, login_mount_point,
                              gpfs_mount_point, ".showq.json.gz", opts.options.dry_run)
                nagios_user_count += 1
            except Exception:
                logger.error("Could not store pickle file for user %s" % (user))
                nagios_no_store += 1

        stats["store_users"] = nagios_user_count
        stats["store_fail"] = nagios_no_store
        stats["store_fail_critical"] = STORE_LIMIT_CRITICAL
    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("dshowq finished", stats)


if __name__ == '__main__':
    main()
