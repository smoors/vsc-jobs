#!/usr/bin/env python
##
#
# Copyright 2013-2013 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
##
"""
dcheckjob.py requests all idle (blocked) jobs from Moab and stores the result in a CheckjobInfo structure in each
users pickle directory.

@author Andy Georges
"""
import sys
import time

from vsc.administration.user import cluster_user_pickle_location_map, cluster_user_pickle_store_map
from vsc.accountpage.client import AccountpageClient
from vsc.config.base import VscStorage
from vsc.filesystem.gpfs import GpfsOperations
from vsc.jobs.moab.checkjob import SshCheckjob, CheckjobInfo
from vsc.utils import fancylogger
from vsc.utils.fs_store import store_on_gpfs
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

#Constants
NAGIOS_CHECK_INTERVAL_THRESHOLD = 30 * 60  # 30 minutes

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

STORE_LIMIT_CRITICAL = 5

# FIXME: common
def get_pickle_path(location, user_id, rest_client):
    """Determine the path (directory) where the pickle file qith the queue information should be stored.

    @type location: string
    @type user_id: string

    @param location: indication of the user accesible storage spot to use, e.g., home or scratch
    @param user_id: VSC user ID
    @param rest_client: VscAccountpageClient instance

    @returns: tuple of (string representing the directory where the pickle file should be stored,
                        the relevant storing function in vsc.utils.fs_store).
    """
    return cluster_user_pickle_location_map[location](user_id).pickle_path()


class MasterSshCheckjob(SshCheckjob):
    """
    ssh into delcatty's master to run the showq command there for fetching information from other clusters
    """
    def __init__(self, target_master, target_user, *args, **kwargs):
        """Initialisation."""
        super(MasterSshCheckjob, self).__init__(*args, **kwargs)
        self.target_master = target_master
        self.target_user = target_user

    def _command(self, path, master):
        """
        Got through master15 instead of the master you wish to interrogate
        """
        return super(MasterSshCheckjob, self)._command("sudo %s" % (path,), "%s@%s" % (self.target_user, self.target_master))

def main():
    # Collect all info

    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'hosts': ('the hosts/clusters that should be contacted for job information', None, 'extend', []),
        'location': ('the location for storing the pickle file: home, scratch', str, 'store', 'home'),
        'location': ('the location for storing the pickle file: delcatty, muk', str, 'store', 'delcatty'),
        'access_token': ('the token that will allow authentication against the account page', None, 'store', None),
        'account_page_url': ('',None, 'store', None),
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
            checkjob_path = opts.configfile_parser.get(host, "checkjob_path")
            clusters[host] = {
                'master': master,
                'path': checkjob_path
            }

        checkjob = MasterSshCheckjob(
            opts.options.target_master,
            opts.options.target_user,
            clusters,
            cache_pickle=True,
            dry_run=opts.options.dry_run)

        (job_information, reported_hosts, failed_hosts) = checkjob.get_moab_command_information()

        active_users = job_information.keys()

        logger.debug("Active users: %s" % (active_users))
        logger.debug("Checkjob information: %s" % (job_information))

        nagios_user_count = 0
        nagios_no_store = 0

        stats = {}

        for user in active_users:
            path = get_pickle_path(opts.options.location, user, rest_client)
            try:
                user_queue_information = CheckjobInfo({user: job_information[user]})
                store_on_gpfs(user, path, "checkjob", user_queue_information, gpfs, login_mount_point,
                        gpfs_mount_point, ".checkjob.json.gz", opts.options.dry_run)
                nagios_user_count += 1
            except Exception:
                logger.exception("Could not store cache file for user %s" % (user))
                nagios_no_store += 1
        stats["store_users"] = nagios_user_count
        stats["store_fail"] = nagios_no_store
        stats["store_fail_critical"] = STORE_LIMIT_CRITICAL
    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("dcheckjob finished", stats)


if __name__ == '__main__':
    main()
