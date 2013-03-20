#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2013-2013 Ghent University
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
Initialize vsc package.
The vsc namespace is used in different folders allong the system
so explicitly declare this is also the vsc namespace

@author: Stijn De Weirdt (Ghent University)
"""
import pkg_resources
pkg_resources.declare_namespace(__name__)
