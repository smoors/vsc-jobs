#!/usr/bin/env python
##
#
# Copyright 2009-2012 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
##
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
from vsc.config.base import VscStorage
from vsc.filesystem.gpfs import GpfsOperations
from vsc.jobs.moab.showq import Showq
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.entities import VscLdapGroup, VscLdapUser
from vsc.ldap.filters import InstituteFilter
from vsc.ldap.utils import LdapQuery
from vsc.utils import fancylogger
from vsc.utils.fs_store import store_on_gpfs
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption


#Constants
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

DEFAULT_VO = 'gvo00012'

STORE_LIMIT_CRITICAL = 5

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()


def collect_vo_ldap(active_users):
    """Determine which active users are in the same VO.

    @type active_users: list of strings

    @param active_users: the users for which there currently are jobs running

    Generates a mapping between each user that belongs to a VO for which a member has jobs running and the active users
    from that VO. If the user belongs to the default VO, he cannot see any information of the other users from this VO.

    @return: dict with vo IDs as keys (default VO members are their own VO) and dicts mapping uid to gecos as values.
    """
    LdapQuery(VscConfiguration())
    ldap_filter = InstituteFilter('antwerpen') | InstituteFilter('brussel') | InstituteFilter('gent') | InstituteFilter('leuven')

    vos = [g for g in VscLdapGroup.lookup(ldap_filter) if g.group_id.startswith('gvo')]
    members = dict([(u.user_id, u) for u in VscLdapUser.lookup(ldap_filter)])
    user_to_vo_map = dict([(u, vo) for vo in vos for u in vo.memberUid])

    user_maps_per_vo = {}
    found = set()
    for user in active_users:

        # If we already have a mapping for this user, we need not add him again
        if user in found:
            continue

        # find VO of this user
        vo = user_to_vo_map.get(user, None)
        if vo:
            if vo.group_id == DEFAULT_VO:
                logger.debug("user %s belongs to the default vo %s" % (user, vo.group_id))
                found.add(user)
                name = members[user].gecos
                user_maps_per_vo[user] = {user: name}
            else:
                user_map = dict([(uid, members[uid].gecos) for uid in vo.memberUid and uid in active_users])
                for uid in user_map:
                    found.add(uid)
                user_maps_per_vo[vo.group_id] = user_map
                logger.debug("added userMap for the vo %s" % (vo.group_id))
        # ignore users not in any VO (including default VO)

    return (found, user_maps_per_vo)


def determine_target_information(information, active_users, queue_information):
    """Determine for the given information type, what should be stored for which users."""

    logger.debug("Determining target information for %s" % (information,))

    if information == 'user':
        user_info = dict([(u, {u: ""}) for u in active_users])  # FIXME: faking it
        return (active_users, dict([(user, {user: queue_information[user]}) for user in active_users]), user_info)
    elif information == 'vo':
        (all_target_users, user_maps_per_vo) = collect_vo_ldap(active_users)

        target_queue_information = {}
        for vo in user_maps_per_vo.values():
            filtered_queue_information = dict([(user_id, queue_information[user_id]) for user_id in vo if user_id in queue_information])
            target_queue_information.update(dict([(user_id, filtered_queue_information) for user_id in vo]))

        return (all_target_users, target_queue_information, user_maps_per_vo)
    elif information == 'project':
        return (None, None, None)


def get_pickle_path(location, user_id):
    """Determine the path (directory) where the pickle file with the queue information should be stored.

    @type location: string
    @type user_id: string

    @param location: indication of the user accessible storage spot to use, e.g., home or scratch
    @param user_id: VSC user ID

    @returns: tuple of (string representing the directory where the pickle file should be stored,
                        the relevant storing function in vsc.utils.fs_store).
    """
    return cluster_user_pickle_location_map[location](user_id).pickle_path()

def main():
    # Collect all info

    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'hosts': ('the hosts/clusters that should be contacted for job information', None, 'extend', []),
        'information': ('the sort of information to store: user, vo, project', None, 'store', 'user'),
        'location': ('the location for storing the pickle file: gengar, muk', str, 'store', 'gengar'),
    }

    opts = ExtendedSimpleOption(options)

    try:
        LdapQuery(VscConfiguration())
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
        showq = Showq(clusters, cache_pickle=True, dry_run=opts.options.dry_run)

        logger.debug("Getting showq information ...")

        (queue_information, reported_hosts, failed_hosts) = showq.get_moab_command_information()
        timeinfo = time.time()

        active_users = queue_information.keys()

        logger.debug("Active users: %s" % (active_users))
        logger.debug("Queue information: %s" % (queue_information))

        # We need to determine which users should get an updated pickle. This depends on
        # - the active user set
        # - the information we want to provide on the cluster(set) where this script runs
        # At the same time, we need to determine the job information each user gets to see
        (target_users, target_queue_information, user_map) = determine_target_information(opts.options.information,
                                                                                        active_users,
                                                                                        queue_information)

        nagios_user_count = 0
        nagios_no_store = 0

        stats = {}

        for user in target_users:
            try:
                path = get_pickle_path(opts.options.location, user)
                user_queue_information = target_queue_information[user]
                user_queue_information['timeinfo'] = timeinfo
                store_on_gpfs(user, path, "showq", (user_queue_information, user_map[user]), gpfs, login_mount_point,
                            gpfs_mount_point, ".showq.json.gz", opts.options.dry_run)
                nagios_user_count += 1
            except (UserStorageError, FileStoreError, FileMoveError), err:
                logger.error("Could not store pickle file for user %s" % (user))
                nagios_no_store += 1

        stats["store+users"] = nagios_user_count
        stats["store_fail"] = nagios_no_store
        stats["store_fail_critical"] = STORE_LIMIT_CRITICAL
    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("dshowq finished", stats)


if __name__ == '__main__':
    main()
