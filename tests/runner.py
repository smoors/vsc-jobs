#!/usr/bin/env python
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
@author: Andy Georges (Ghent University)
"""

import os
import sys

# make sure what we're testing is first in the path
sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 'lib'))
sys.path.insert(0, '')

import tests.qstat as qstat
import unittest


from vsc.utils import fancylogger
fancylogger.logToScreen(enable=False)

suite = unittest.TestSuite([x.suite() for x in (qstat,)])

try:
    import xmlrunner
    rs = xmlrunner.XMLTestRunner(output="test-reports").run(suite)
except ImportError, err:
    rs = unittest.TextTestRunner().run(suite)

if not rs.wasSuccessful():
    sys.exit(1)
