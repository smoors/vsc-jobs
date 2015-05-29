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
@author: Andy Georges (Ghent University)
"""

from unittest import TestCase, TestLoader

from vsc.jobs.pbs.qstat import ranges, convert_to_range, normalise_exec_host, normalise_time, transform_info


class TestQstatWrapper(TestCase):
    """
    Tests for the qstat -f transformers
    """

    def test_ranges(self):
        """
        test the conversion of a list of numbers to a list tuples with the ranges of the original numbers
        """
        list1 = [1, 2, 3, 6, 7, 8, 12, 23, 25, 26]
        expected1 = [(1, 3), (6, 8), (12, 12), (23, 23), (25, 26)]

        self.assertEqual(list(ranges(list1)), expected1)

    def test_convert_to_range(self):
        """
        test the conversion of a lost of number to the canonical string indicating the ranges
        """

        list1 = [1, 2, 3, 6, 7, 8, 12, 23, 25, 26]
        list2 = list1
        list2.reverse()

        self.assertEqual(convert_to_range(list1), "1-3,6-8,12,23,25-26")
        self.assertEqual(convert_to_range(list1), convert_to_range(list2))

    def test_normalise_time(self):
        """
        Test the conversion of a string [[[DD:]HH:]MM:]SS into seconds.
        """

        date_info1 = (1, 2, 3, 4)
        date1 = "%2d:%2s:%2d:%2d" % date_info1

        dates = [date1]
        date_infos = [date_info1]

        for d in zip(dates, date_infos):
            self.assertEqual(
                normalise_time(date1),
                sum([t*s for (t, s) in zip(date_info1, (24*60*60, 60*60, 60, 1))])
            )

    def test_normalise_exec_host(self):
        """
        test the conversion of a list containing one or more strings of the form nodenameXYZ/C+nodenameUVW/D+...
        to nodenameXYZ/A-C,nodenameUVW/D-E,...
        """
        list1 = ["node2400/0+node2400/1+node2400/2+node2401/3+node2401/4+node2402/5"]
        expected1 = "node2400/0-2,node2401/3-4,node2402/5"

        self.assertEqual(normalise_exec_host(list1), expected1)       




def suite():
    """ returns all the testcases in this module """
    return TestLoader().loadTestsFromTestCase(TestQstatWrapper)
