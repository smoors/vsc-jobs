#
# Copyright 2016-2016 Ghent University
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
@author: stdweird
"""
import os
import re
import sys
from vsc.install.testing import TestCase

from vsc.jobs.pbs.submitfilter import parse_resources, parse_resources_nodes, SubmitFilter, \
                                parse_commandline_string, parse_commandline_list, \
                                get_warnings, reset_warnings, \
                                _parse_mem_units, parse_mem, PBS_DIRECTIVE_PREFIX_DEFAULT, \
                                cluster_from_options

from vsc.jobs.pbs.clusterdata import DEFAULT_SERVER_CLUSTER, MASTER_REGEXP, CLUSTERDATA

RESOURCE_NODES = [
    {
        'orig': '1',
        '_nrnodes': 1,
        '_nrcores' : 1,
        '_ppn': 1,
        'nodes': '1:ppn=1',
    },
    {
        'orig': '1:ppn=1',
        '_nrnodes': 1,
        '_nrcores' : 1,
        '_ppn': 1,
        'nodes': '1:ppn=1',
    },
    {
        'orig': '1:ppn=2',
        '_nrnodes': 1,
        '_nrcores' : 2,
        '_ppn': 2,
        'nodes': '1:ppn=2',
    },
    {
        'orig': '2:ppn=4',
        '_nrnodes': 2,
        '_nrcores' : 8,
        '_ppn': 4,
        'nodes': '2:ppn=4',
    },
    {
        # gengar
        'orig': '2:ppn=all',
        '_nrnodes': 2,
        '_nrcores' : 16,
        '_ppn': 8,
        'nodes': '2:ppn=8',
    },
    {
        # gengar
        'orig': '2:ppn=half',
        '_nrnodes': 2,
        '_nrcores' : 8,
        '_ppn': 4,
        'nodes': '2:ppn=4',
    },
    {
        'orig': 'node1:ppn=4+node2:ppn=4',
        '_nrnodes': 2,
        '_nrcores' : 8,
        '_ppn': 4,
        'nodes': 'node1:ppn=4+node2:ppn=4',
    },
    {
        'orig': 'node1:ppn=full+node2:ppn=half',
        '_nrnodes': 2,
        '_nrcores' : 8+4,
        '_ppn': 6,
        'nodes': 'node1:ppn=8+node2:ppn=4',
    },
    {
        'orig': 'node1:ppn=full+node2:super:ppn=half:cool+3:whatever:ppn=10:sure',
        '_nrnodes': 5,
        '_nrcores' : 8+4+3*10,
        '_ppn': 8,
        'nodes': 'node1:ppn=8+node2:super:ppn=4:cool+3:whatever:ppn=10:sure',
    },
    {
        'orig': 'node1:ppn=whatever+node2:super:ppn=half:cool+3:whatever:ppn=woohoo:sure',
        '_nrnodes': 5,
        '_nrcores' : 1+4+3*1,
        '_ppn': 1,
        'nodes': 'node1:ppn=1+node2:super:ppn=4:cool+3:whatever:ppn=1:sure',
    },


]

RESOURCE_MEM = [
    {
        'orig': 'pmem=100k,vmem=200k',
        'pmem': '100k',
        '_pmem': 100*2**10,
        'vmem': '200k',
        '_vmem': 200*2**10,
        'resources' : 'pmem=100k,vmem=200k',
    },
    {
        'orig': 'pmem=half,vmem=all',
        'pmem': '',
        '_pmem': 1,
        'vmem': '',
        '_vmem': 1,
        'resources' : 'pmem=,vmem=',
    },
    {
        'orig': '',
        'mem': '',
        '_mem': 1,
        'resources' : '',
    },
]

RESOURCES = [
    {
        # gengar
        'orig': 'vmem=100k,nodes=1:ppn=8+3:ppn=6:extra+node3:super:ppn=half,pmem=200',
        '_nrnodes': 5,
        '_nrcores' : 8+3*6+4,
        '_ppn': 6,
        'nodes': '1:ppn=8+3:ppn=6:extra+node3:super:ppn=4',
        'vmem': '100k',
        'pmem': '200',
        '_vmem': 100*2**10,
        '_pmem': 200,
        'resources': 'vmem=100k,nodes=1:ppn=8+3:ppn=6:extra+node3:super:ppn=4,pmem=200',
    },
]

SCRIPTS = ["""#!/bin/sh
#
#
#PBS -N testrun
#PBS -o output_testrun.txt -l nodes=5:ppn=all,pmem=half
#PBS -e error_testrun.txt
#PBS -l walltime=11:25:00
#PBS -l vmem=500mb
#PBS -m bea
#PBS -q short
#
cd $VSC_HOME
##logs to stderr by default, redirect this to stdout
./pfgw64s 42424242_1t.txt 2>> $VSC_SCRATCH/testrun.42424242.out
""",
]

class TestSubmitfilter(TestCase):

    def setUp(self):
        reset_warnings()
        for env in ['PBS_DEFAULT', 'PBS_DPREFIX', 'VSC_NODE_PARTITION']:
            if env in os.environ:
                del os.environ[env]

        # Add gengar cluster for testing purposes
        CLUSTERDATA['gengar'] = {
            'PHYSMEM': 16439292 << 10,
            'TOTMEM': 37410804 << 10,
            'NP': 8,
            'NP_LCD': 2,
        }

        super(TestSubmitfilter, self).setUp()

    def tearDown(self):
        CLUSTERDATA.pop('gengar')
        super(TestSubmitfilter, self).tearDown()

    def test_nodesfilter(self):
        """Test parse_resources_nodes"""
        for testdata in RESOURCE_NODES:
            orig = testdata.pop('orig')
            resources = {}
            nodes = parse_resources_nodes(orig, 'gengar', resources)
            self.assertEqual('nodes=' + resources['nodes'], nodes,
                             msg='nodes returned is equal to nodes=resources[nodes] (nodes=%s == %s)' % (resources['nodes'], nodes))
            self.assertEqual(resources, testdata, msg='generated resources equal to expected (expected %s generated %s; orig %s)' %
                             (testdata, resources, orig))

        # up to now, 2 warnings
        warnings = get_warnings()
        self.assertEqual(warnings,
                         ['Warning: unknown ppn (whatever) detected, using ppn=1',
                          'Warning: unknown ppn (woohoo) detected, using ppn=1',
                         ], msg="expected warnings after processing examples %s" % (warnings))

        reset_warnings()

    def test_resources(self):
        """Test parse_resources_nodes"""
        for testdata in RESOURCES:
            orig = testdata.pop('orig')
            rs=testdata.pop('resources')
            resources = {}
            resourcestxt = parse_resources(orig, 'gengar', resources)
            self.assertEqual(rs, resourcestxt, msg='resources text returned is equal to resources[resources] (%s == %s)' % (rs, resourcestxt))
            self.assertEqual(resources, testdata, msg='generated resources equal to expected (expected %s generated %s; orig %s rs %s)' %
                             (testdata, resources, orig, rs))

    def test_dprefix(self):
        """Test dprefix and usage in parseline and make_header"""
        h = SubmitFilter([],[])

        self.assertEqual(PBS_DIRECTIVE_PREFIX_DEFAULT, '#PBS',
                         msg='PBS_DIRECTIVE_PREFIX_DEFAULT %s' % PBS_DIRECTIVE_PREFIX_DEFAULT)
        self.assertEqual(h.dprefix, PBS_DIRECTIVE_PREFIX_DEFAULT,
                         msg='PBS_DIRECTIVE_PREFIX_DEFAULT set as default dprefix')

        # test parseline with default dprefix
        self.assertEqual(h.parseline('regular line of text'), None, msg='non-header returns None')
        self.assertEqual(h.parseline('# sldjldkfjkg'), [], msg='regular header returns empty list')
        self.assertEqual(h.parseline('     '), [], msg='whitespace returns empty list')

        res = h.parseline('#PBS -x y=z -a b=c,d=e,f,g # some comment')
        self.assertEqual(h.parseline('#PBS -x y=z -a b=c,d=e,f,g # some comment'),
                         [('x', 'y=z'), ('a', 'b=c,d=e,f,g')], msg='PBS header with 2 options and comment (%s)' % (res,))
        self.assertEqual(h.make_header('-a','b'), '#PBS -a b', 'Generate correct header')

        # drpefix from enviornment
        os.environ['PBS_DPREFIX'] = '#ENVDPREFIX'
        h1 = SubmitFilter([],[])
        self.assertEqual(h1.dprefix, '#ENVDPREFIX', msg='dprefix from environment wins')

        # different dprefix from commandline
        h2 = SubmitFilter(['-C', '#SOMETHINGELSE'], [])
        self.assertEqual(h2.dprefix, '#SOMETHINGELSE', msg='cmdline dprefix wins')

        # very different dprefix
        self.assertEqual(h2.parseline('#PBS -x y=z -a b'), [], 'new dprefix, this is now just a comment')
        self.assertEqual(h2.parseline('#SOMETHINGELSE -x y=z -a b'), [('x', 'y=z'), ('a','b')], 'new dprefix, this is now just a comment')
        self.assertEqual(h2.make_header('-a','b'), '#SOMETHINGELSE -a b', 'Generate correct header with modified dprefix')

        del os.environ['PBS_DPREFIX']

    def test_commandline_string_list(self):
        """Test parse_commandline_string and parse_commandline_list"""
        def t(data, val, msg):
            res=parse_commandline_string(data)
            self.assertEqual(res, val, "%s: text %s res %s" % (msg, data, res))

        def t2(data, val, msg):
            res=parse_commandline_list(data)
            self.assertEqual(res, val, "%s: list %s res %s" % (msg, data, res))

            newdata = " ".join(data)
            t(newdata, val, msg)

        t2(["-a", "5", "-b", "hello", "-c"], [("a", "5"), ("b", "hello"), ("c", None)], "simple test")
        t2(["-a", "5:6=7,8:9", "-b"], [("a", "5:6=7,8:9"), ("b", None)], "test with non alphanum values")

        t2(["-a"], [("a", None)], "test single non-value arg")
        t2(["-a","-b"], [("a", None), ("b", None)], "test 2 non-value args")
        t2(["-a","-b","-c"], [("a", None), ("b", None), ("c", None)], "test 3 non-value args")
        t2(["-a","-b","-c","-d"], [("a", None), ("b", None), ("c", None), ("d", None)], "test 4 non-value args")

        t("  -a    -b   6587   -c    ", [("a", None), ("b", "6587"), ("c", None)], "test line with lots of whitespace")

    def test_parse_mem_units(self):
        """Test _parse_mem_units"""
        data = [("", None), ("123Xb", None), ("hallo", None),
                ("1", 1), ("123b", 123), ("123w", 123), ("123B", 123),
                ("123kb", 123*1024), ("123kw", 123*1024),
                ("123Kb", 123*1024), ("123Kw", 123*1024),
                ("123mb", 123*1024**2), ("123mw", 123*1024**2),
                ("123Mb", 123*1024**2), ("123Mw", 123*1024**2),
                ("123gb", 123*1024**3), ("123gw", 123*1024**3),
                ("123Gb", 123*1024**3), ("123Gw", 123*1024**3),
                ("123tb", 123*1024**4), ("123tw", 123*1024**4),
                ("123Tb", 123*1024**4), ("123Tw", 123*1024**4),
        ]
        for txt, value in data:
            self.assertEqual(_parse_mem_units(txt), value,
                             msg="Tested '%s' expected %s bytes %s" % (txt, value, _parse_mem_units(txt)))

    def test_parse_mem(self):
        """Test parse_mem"""
        # test with delcatty cluster
        cluster = 'delcatty'
        overhead = 2842984448
        allvmem = 78367821824 - overhead
        allpmem = 67630407680 - overhead
        data = [
            ('pmem=100k', {'pmem': '100k', '_pmem': 100*2**10}, 'pmem=100k'),
            ('vmem=half', {'vmem': '%s' % int(allvmem/2), '_vmem': int(allvmem/2)}, 'vmem=%s' % int(allvmem/2)),
            ('pmem=full', {'pmem': '%s' % int(allpmem), '_pmem': int(allpmem)}, 'pmem=%s' % int(allpmem)),
            ('vmem=all', {'vmem': '%s' % int(allvmem), '_vmem': int(allvmem)}, 'vmem=%s' % int(allvmem)),
        ]
        for txt, rsc, newtxt in data:
            k, v = txt.split('=')
            resources = {}
            mtxt = parse_mem(k, v, cluster, resources)
            self.assertEqual(mtxt, newtxt,
                             msg='new generated mem text %s (%s old txt %s)' % (newtxt, mtxt, txt))
            self.assertEqual(resources, rsc,
                             msg='expected resources updated %s' % rsc)

    def test_parse(self):
        h = SubmitFilter(
            ['-q', 'verylong'],
            [x + "\n" for x in SCRIPTS[0].split("\n")]
        )

        # stdin is an iterator
        self.assertTrue(hasattr(h.stdin, 'next'), "stdin is an iterator")

        h.parse_header()

        self.assertEqual(h.header,
                         ['#!/bin/sh', '#', '#',
                          '#PBS -N testrun',
                          '#PBS -o output_testrun.txt -l nodes=5:ppn=all,pmem=half',
                          '#PBS -e error_testrun.txt',
                          '#PBS -l walltime=11:25:00',
                          '#PBS -l vmem=500mb',
                          '#PBS -m bea',
                          '#PBS -q short',
                          '#',
                      ], msg="Found header %s" % h.header)

        self.assertEqual(h.prebody, "cd $VSC_HOME\n", msg="found prebody '%s'" % h.prebody)
        self.assertEqual(h.dprefix, '#PBS', msg="found expected headers dprefix %s" %  h.dprefix)

        self.assertEqual(h.stdin.next(),
                         "##logs to stderr by default, redirect this to stdout\n",
                         msg="stdin at expected position")

        self.assertEqual(h.allopts,
                         [('N', 'testrun'),
                          ('o', 'output_testrun.txt'),
                          ('l', 'nodes=5:ppn=all,pmem=half'),
                          ('e', 'error_testrun.txt'),
                          ('l', 'walltime=11:25:00'),
                          ('l', 'vmem=500mb'),
                          ('m', 'bea'),
                          ('q', 'short'),
                          ('q', 'verylong'),
                          ], msg="found alloptions in order %s" % h.allopts)

        self.assertEqual(h.occur,
                         [3, 4, 4, 5, 6, 7, 8, 9, None],
                         msg="expected ordered occurence of options %s" % h.occur)

    def test_cluster_from_options(self):
        """Test cluster from options with ugent/vsc regexp"""

        self.assertEqual(cluster_from_options([
            ('l', 'x'),
            ('q', 'long@master1.something.gent.vsc'),
            ('m', 'more'),
            ('q', 'short@master.whatever.os'),
            ('N', None)
        ], MASTER_REGEXP), 'whatever',
                         msg="found expected cluster based on last queue option")

        os.environ['PBS_DEFAULT'] = "master10.mycluster.gent.vsc"
        self.assertEqual(cluster_from_options([
            ('l', 'x'),
            ('q', 'long'),
            ('m', 'more'),
            ('q', 'short'),
            ('N', None)
        ], MASTER_REGEXP), 'mycluster',
                         msg="found expected cluster based on PBS_DEFAULT environment variable %s" % os.environ['PBS_DEFAULT'])


        self.assertEqual(get_warnings(), [], msg='no cluster warnings')

        os.environ['PBS_DEFAULT'] = "master10.notgent.vsc"
        self.assertEqual(cluster_from_options([
            ('l', 'x'),
            ('q', 'long'),
            ('m', 'more'),
            ('q', 'short'),
            ('N', None)
        ], MASTER_REGEXP), DEFAULT_SERVER_CLUSTER,
                         msg="no cluster found, fallback to DEFAULT_SERVER_CLUSTER %s" % DEFAULT_SERVER_CLUSTER)

        self.assertEqual(get_warnings(), [
            'Unable to determine clustername, using default delcatty (queue short, PBS_DEFAULT %s)' % os.environ['PBS_DEFAULT'],
        ], msg='no cluster found, warnings generated %s' %(get_warnings()))

        del os.environ['PBS_DEFAULT']

    def test_gather_state(self):
        """Test gather_state"""

        h = SubmitFilter(
            ['-q', 'verylong'],
            [ x + "\n" for x in SCRIPTS[0].split("\n")]
        )
        h.parse_header()

        os.environ['PBS_DEFAULT'] = "master15.delcatty.gent.vsc"
        overhead = 2842984448
        allpmem = 67630407680 - overhead

        state, newopts = h.gather_state(MASTER_REGEXP)

        self.assertTrue('l' in state, msg='state retruned by gather_state always contains key "l"')

        self.assertEqual(state, {
            'e': 'error_testrun.txt',
            'm': 'bea',
            'l': {
                '_nrcores': 80,
                '_ppn': 16,
                'vmem': '500mb',
                '_vmem': 500*2**20,
                '_nrnodes': 5,
                'nodes': '5:ppn=16',
                'walltime': '11:25:00',
                'pmem': '%s' % (allpmem/2),
                '_pmem': allpmem/2,
            },
            'o': 'output_testrun.txt',
            'N': 'testrun',
            'q': 'verylong',
            '_cluster': 'delcatty'
        }, msg="expected state %s" % state)

        self.assertEqual(newopts, [
            'testrun',
            'output_testrun.txt',
            'nodes=5:ppn=16,pmem=%s' % (allpmem/2),
            'error_testrun.txt',
            'walltime=11:25:00',
            'vmem=500mb',
            'bea',
            'short',
            'verylong',
        ], msg="expected newopts %s" % newopts)

        del os.environ['PBS_DEFAULT']
