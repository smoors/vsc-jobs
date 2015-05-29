#!/usr/bin/python
# -*- coding: latin-1 -*-
##
# Copyright 2015-2015 Ghent University
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

logger = fancylogger.getLogger(sys.args[0])


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

    transform_info(current_job, opts.options.information)


main()
