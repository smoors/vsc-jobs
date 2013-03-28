#!/usr/bin/python
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
mycheckjob shows the contents of the information that was saved to the
.checkjob.pickle file in the user's home directory.

This mimics the result of Moab's checkjob command, but without the
risks of letting users see job information of jobs that are not theirs.
"""
import cPickle
import os
import pwd
import sys
import time

from vsc.administration.user import MukUser, VscUser
from vsc.utils import fancylogger
from vsc.utils.generaloption import simple_option

MAXIMAL_AGE = 60 * 30  # 30 minutes

logger = fancylogger.getLogger("myshowq")
fancylogger.logToScreen(True)
fancylogger.setLogLevelWarning()


def checkjob_data_location(user_name, location):
    """Retrieve the pickled data form the right file.

    @type user_name: string
    @type location: string

    @param user_name: VSC user name (vscxyzuv)
    @param location: string defining the location of the pickle file
        - home: user's home directory
        - scratch: user's personal fileset on muk

    @returns: absolute path to the pickle file
    """
    if location == 'home':
        return os.join(VscUser(user_name).pickle_path(), '.checkjob.pickle')
    elif location == 'scratch':
        return os.join(MukUser(user_name).pickle_path(), '.checkjob.pickle')


def read_checkjob_data(path):
    """Read the data from the pickle file.

    @type path: string

    @param path: absolute path to the pickle file

    @returns: (timeinfo, CheckjobInfo instance (for a single user)) or (0, None) in case of failure.
    """
    try:
        f = open(path, 'r')
        (timeinfo, checkjob) = cPickle.load(f)
        f.close()
    except Exception, err:
        logger.error("Cannot read pickle file", err)
        return (0, None)

    return (timeinfo, checkjob)


def main():

    options = {
        'jobid': ('jobid', 'Fully qualified identification of the job', None, 'store', None),
        'location': ('the location for storing the pickle file: home, scratch', str, 'store', 'home'),
    }

    opts = simple_option(options)

    my_uid = os.geteuid()
    my_name = pwd.getpwuid(my_uid)[0]

    (timeinfo, checkjob) = read_checkjob_data(my_name, opts.options.location)

    age = time.time() - timeinfo

    if age > MAXIMAL_AGE:
        print "Job information is older than %d minutes (%f hours). Information may not be relevant any longer" % (age / 60, age / 60.0 / 60.0)

    print checkjob.display(opts.options.jobid)


if __name__ == '__main__':
    main()


