#!/usr/bin/python
#
# Copyright 2013-2016 Ghent University
#
# This file is part of vsc-jobs,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
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
mycheckjob shows the contents of the information that was saved to the
.checkjob.json.gz file in the user's home directory.

This mimics the result of Moab's checkjob command, but without the
risks of letting users see job information of jobs that are not theirs.

@author: Andy Georges (Ghent University)
"""
import cPickle
import os
import time

from pwd import getpwuid
from vsc.config.base import VscStorage
from vsc.utils import fancylogger
from vsc.utils.cache import FileCache
from vsc.utils.generaloption import simple_option
from vsc.utils.cache import FileCache

MAXIMAL_AGE = 60 * 30  # 30 minutes

logger = fancylogger.getLogger("mycheckjob")
fancylogger.logToScreen(True)
fancylogger.setLogLevelWarning()


def read_cache(path):
    """
    Unpickle the file and fill in the resulting datastructure.
    """
    try:
        cache = FileCache(path)
    except:
        print "Failed to load checkjob information from %s" % (path,)

    res = cache.load('checkjob')
    if res[0] < (time.time() - MAXIMAL_AGE):
        print "The data in the checkjob cache may be outdated. Please contact your admin to look into this."

    return res[1]  # CheckjobInfo


def main():

    options = {
        'jobid': ('Fully qualified identification of the job', None, 'store', None),
        'location_environment': ('the location for storing the pickle file depending on the cluster', str, 'store', 'VSC_SCRATCH_DELCATTY'),
    }
    opts = simple_option(options, config_files=['/etc/mycheckjob.conf'])

    storage = VscStorage()
    user_name = getpwuid(os.getuid())[0]

    mount_point = storage[opts.options.location_environment].login_mount_point
    path_template = storage.path_templates[opts.options.location_environment]['user']
    path = os.path.join(mount_point, path_template[0], path_template[1](user_name), ".checkjob.json.gz")

    checkjob_info = read_cache(path)

    print checkjob_info.display(opts.options.jobid)

if __name__ == '__main__':
    main()

