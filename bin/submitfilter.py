#!/usr/bin/python
# #
# Copyright 2013-2013 Ghent University
#
# This file is part of vsc-jobs,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# http://github.com/hpcugent/vsc-jobs
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
# #
"""
Filters scripts submitted by qsub,
adds default values and command line parameters to the script
for processing by pbs

@author: Jens Timmerman (Ghent University)
@author: Stijn De Weirdt (Ghent University)
"""

# -mail options parsen
# -vmem option parsen
# -cores parsen (voor vmem)
import sys
import re
import os
from vsc.utils.generaloption import PassThroughOptionParser

DEFAULT_SERVER_CLUSTER = 'gengar'

DEFAULT_SERVER = "default"

# these amounts are in kilobytes as reported by pbsnodes
CLUSTERDATA = {
    'delcatty': {
        'PHYSMEM': 66045320 << 10,
        'TOTMEM': 87016832 << 10,
        'NP': 16,
        },
    'gengar': {
        'PHYSMEM': 16439292 << 10,
        'TOTMEM': 37410804 << 10,
        'NP': 8,
        },
    'gastly': {
        'PHYSMEM': 12273152 << 10,
        'TOTMEM': 33244664 << 10,
        'NP': 8,
        },
    'haunter': {
        'PHYSMEM': 12273152 << 10,
        'TOTMEM': 33244664 << 10,
        'NP': 8,
        },
    'gulpin': {
        'PHYSMEM': 66093364 << 10,
        'TOTMEM': 87064892 << 10,
        'NP': 32,
        },
    'raichu': {
        'PHYSMEM': 32973320 << 10,
        'TOTMEM': 53944832 << 10,
        'NP': 16,
        },
    'muk': {
        'PHYSMEM': 66068964 << 10,
        'TOTMEM': 99623388 << 10,
        'NP': 16,
        },
    'dugtrio': {
        'TOTMEM': 1395439 << 20,  # total amount of ram in dugtrio cluster
        'DEFMAXNP': 48,  # default maximum np in case of ppn=full
        },
    }


MIN_VMEM = 1536 << 20  # minimum amount of ram in our machines.


def main(arguments=sys.argv):
    """
    main method
    """
    # regexes needed here
    mailreg = re.compile(r"^#PBS\s+-m\s.+")
    vmemreg = re.compile(r'^#PBS\s+-l\s+[^#]*?vmem=(?P<vmem>[^\s]*)')
    # multiline to replace header in single sub
    ppnreg = re.compile(r'^#PBS\s+-l\s+[^#]*?nodes=.+?:ppn=(?P<ppn>\d+|all|half)', re.M)
    serverreg = re.compile(r'master\d*(?:-moab\d*)?\.(?P<server>[^.]+)\.(?:gent\.vsc|os)')
    optsppnreg = re.compile(r'nodes=(?P<nodes>\d+)[^#,\s]*ppn=(?P<ppn>\d+)')

    optsvmemreg = re.compile(r'vmem=(?P<vmem>[^#\s]*)')
    # parse command line options
    parser = PassThroughOptionParser()  # ignore unknown options
    parser.add_option("-m", help="mail option")
    parser.add_option("-q", help="queue/server option")
    # parser.add_option("-h", help="dummy option to prevent printing help",action="count")
    parser.add_option("-l", help="some other options", action="append")

    # parser.add_option()
    (opts, args) = parser.parse_args(arguments)

    vmemDetected = False
    ppnDetected = False
    ppnDetectedinHeader = None
    mailDetected = bool(opts.m)
    serverDetected = False
    noVmemNeeded = False
    opts.server = None
    # process appended results to l
    opts.vmem = None
    opts.ppn = 1
    if opts.l:
        for arg in opts.l:
            match = optsppnreg.match(arg)
            if match:
                opts.ppn = match.group("ppn")
                ppnDetected = True
            match = optsvmemreg.match(arg)
            if match:
                opts.vmem = match.group("vmem")
                vmemDetected = True

    # check for server in options
    if opts.q:
        t = serverreg.match(opts.q)
        if t:
            opts.server = t.group('server')
            serverDetected = True

    # process stdin
    header = ""
    body = ""
    for line in iter(sys.stdin.readline, ''):
        # check if we're still in the preamble
        if not line.startswith('#') and not line.strip() == "":
            # jump out of loop here, we will print the rest later
            body = line  # save this line first
            break
        header += line
        # check if this line is mail
        if not mailDetected:  # if mail not yet found
            opts.m = mailreg.match(line)  # returns None if no match
            mailDetected = bool(opts.m)
        if not vmemDetected:
            opts.vmem = vmemreg.match(line)
            vmemDetected = bool(opts.vmem)
            if vmemDetected:
                opts.vmem = opts.vmem.group("vmem")
        if not ppnDetected:
            t = ppnreg.match(line)
            if t:
                opts.ppn = t.group("ppn")  # returns '' if no match, which evaluates to false
                ppnDetected = bool(opts.ppn)
                ppnDetectedinHeader = t.group(0)  # the whole matched string, to be rewritten
        if not serverDetected:
            t = serverreg.match(line)
            if t:
                opts.server = t.group("server")
                serverDetected = True

    # combine results
    # vmem

    # try to find the server if not set yet
    if not serverDetected and 'PBS_DEFAULT' in os.environ:
        t = serverreg.match(os.environ['PBS_DEFAULT'])
        if t:
            opts.server = t.group("server")
            serverDetected = bool(opts.server)  # opts.server can also be the empty string

    # check whether VSC_NODE_PARTITION environment variable is set
    # used for gulpin/dugtrio

    if 'VSC_NODE_PARTITION' in os.environ:
        header += "\n#PBS -W x=PARTITION:%s\n" % os.environ['VSC_NODE_PARTITION']

    # set defaults
    if not serverDetected:
        opts.server = DEFAULT_SERVER
    if not ppnDetected:
        opts.ppn = 1

    # compute vmem
    if not serverDetected or opts.server in [DEFAULT_SERVER]:
        opts.server = DEFAULT_SERVER_CLUSTER

    if opts.server in CLUSTERDATA:
        cluster = CLUSTERDATA[opts.server]
        try:
            tvmem = int((cluster['PHYSMEM'] + (cluster['TOTMEM'] - cluster['PHYSMEM']) / 2) / cluster['NP'])
        except:
            # something is not defined (eg dugtrio case)
            tvmem = None  # dont set it if not found

        if opts.server in ["dugtrio"]:
            noVmemNeeded = True

        maxvmem = cluster.get('TOTMEM', 0)
        if opts.ppn in ('all', 'half'):
            ppn = cluster.get('NP', cluster.get('DEFMAXNP', 1))
            if opts.ppn == 'half':
                opts.ppn = max(1, int(ppn / 2))
            else:
                opts.ppn = ppn
    else:
        # backup, but should not occur
        tvmem = MIN_VMEM
        maxvmem = 0
        if opts.ppn in ('all', 'half'):
            opts.ppn = 1
        sys.stderr.write("Warning: unknown server (%s) detected, see PBS_DEFAULT. This should not be happening...\n" % opts.server)

    # always (and only here to replace ppn=all or ppn=half
    opts.ppn = int(opts.ppn)

    if not vmemDetected and not noVmemNeeded:
        # compute real vmem needed
        vmem = tvmem * opts.ppn >> 20  # change to mb
        header += "# No vmem limit specified - added by submitfilter (server found: %s)\n#PBS -l vmem=%smb\n" % (opts.server, vmem)
    elif not noVmemNeeded:
        # parse detected vmem to check if to much was asked
        groupvmem = re.search('(\d+)(.*)', opts.vmem)
        intvmem = groupvmem.group(1)
        if intvmem:
            intvmem = int(intvmem)
        else:
            intvmem = 0
        suffix = groupvmem.group(2)
        if suffix:
            reqvmem = suffix.lower()
            if reqvmem.endswith('tb') or reqvmem.endswith('tw'):
                intvmem = intvmem << 40
            if reqvmem.endswith('gb') or reqvmem.endswith('gw'):
                intvmem = intvmem << 30
            if reqvmem.endswith('mb') or reqvmem.endswith('mw'):
                intvmem = intvmem << 20
            if reqvmem.endswith('kb') or reqvmem.endswith('kw'):
                intvmem = intvmem << 10

        if intvmem > maxvmem:
            # warn user that he's trying to request to much vmem
            sys.stderr.write("Warning, requested %sb vmem per node, this is more then the available vmem (%sb), this job will never start.\n" % (intvmem, maxvmem))
    # mail
    if not mailDetected:
        header += "# No mail specified - added by submitfilter\n#PBS -m n\n"

    # ppn in header
    if ppnDetectedinHeader:
        header = re.sub('ppn=(all|half)', 'ppn=%d' % opts.ppn, header)

    print header
    print body

    # print rest of stdin to stdout
    for line in iter(sys.stdin.readline, ''):
        sys.stdout.write(line)

if __name__ == '__main__':
    main()

