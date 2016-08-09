#!/usr/bin/env python
# -*- coding: latin-1 -*-
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
qstat-wrapper prints out job information in a fixed format, where every entry
is placed on a single line, unlike qstat -f, which wraps information on multiple
lines if needed.

@author: Andy Georges (Ghent University)
"""
import sys

from PBSQuery import PBSQuery
from vsc.jobs.pbs.qstat import transform_info
from vsc.utils import fancylogger
from vsc.utils.generaloption import simple_option

logger = fancylogger.getLogger(sys.argv[0])


def main():
    """
    Main script.
    """

    options = {
        "jobid": ("The PBS_JOBID of the job for which we want information", None, "store", None),
        "information": ("Comma-separated list of the job info to print. "
                        "Entries of the format input_key:output_key", None, "store", None),
    }
    opts = simple_option(options)

    if not opts.options.jobid:
        logger.error("jobid is a required option. Bailing.")
        sys.exit(1)

    pquery = PBSQuery()
    current_job = pquery.getjob(opts.options.jobid)

    s = transform_info(current_job, opts.options.information)

    print "\n".join(s)


if __name__ == '__main__':
    main()
