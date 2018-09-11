#
# Copyright 2015-2018 Ghent University
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
Module with submitfilter tools

@author: Stijn De Weirdt (Ghent University)
"""
import logging
import os
import re
import sys

from vsc.jobs.pbs.clusterdata import DEFAULT_SERVER_CLUSTER
from vsc.jobs.pbs.clusterdata import get_cluster_maxppn, get_cluster_mpp

NODES_PREFIX = 'nodes'

PBS_DIRECTIVE_PREFIX_DEFAULT = '#PBS'
# All qsub options are "short" options, so the '-\w+' could probably be '-\w'
PBS_OPTION_REGEXP = re.compile(r"(?:^|\s)-(\w+)(?:(?!\s*(?:\s-\w+|$))\s+(.*?(?=\s*(?:\s-\w+|$))))?")

# TODO: all lower and uppercase combos?
MEM_REGEXP = re.compile(r'^(p|v|pv)?mem')
MEM_VALUE_REG = re.compile(r'^(\d+)(?:(|[kK]|[mM]|[gG]|[tT])[bBw]?)?$')
MEM_VALUE_UNITS = ('', 'k', 'm', 'g', 't')

PMEM = 'pmem'
VMEM = 'vmem'
MEM = 'mem'
FEATURE = 'feature'

_warnings = []


def reset_warnings():
    """Reset the list of warnings"""
    global _warnings
    _warnings = []


def get_warnings():
    """Return the list of warnings"""
    global _warnings
    return _warnings


def warn(*txt):
    """Collect warning, not actually print anything"""
    global _warnings
    _warnings.append(" ".join(txt))


def abort(txt):
    """Write generated warning messages, followed by error message and exit"""
    for warning in ["%s\n" % w for w in get_warnings()]:
        sys.stderr.write(warning)
    sys.stderr.write('Error: ' + txt + '\n')
    sys.stderr.flush()
    sys.exit(1)


class SubmitFilter(object):
    """PBS script processing"""

    def __init__(self, arguments, stdin):
        """
        Parse commandline arguments and header from stdin.

        Sets the header (as list of lines), prebody (as text) and the remainder as iter instance,
        all options and list with index in header where they were found (index None means commandline)
        """

        self.cmdlineopts = parse_commandline_list(arguments)  # list of (opt, val)

        self.dprefix = None
        self.regexp = None

        self.header = []
        self.prebody = ''  # TODO insert it back in stdin iterator and get rid of it?
        self.allopts = []
        self.occur = []

        if isinstance(stdin, (list, tuple)):
            stdin_iter = iter(stdin)
        else:
            stdin_iter = iter(stdin, '')
        self.stdin = stdin_iter

        self.set_pbs_header_regexp()

    def set_pbs_header_regexp(self, dprefix=None):
        """Generate the header regexp, returns the prefix to create new header lines"""
        if dprefix is None:
            cmdlinedict = dict(self.cmdlineopts)
            dprefix = cmdlinedict.get('C', os.environ.get('PBS_DPREFIX', PBS_DIRECTIVE_PREFIX_DEFAULT))

        self.dprefix = dprefix
        self.regexp = re.compile(r"^" + dprefix + r"\s+([^#]*)\s*(?:#|$)")

    def make_header(self, *opts):
        """Return header line (all args are joined with space)"""
        return " ".join([self.dprefix] + list(opts))

    def parseline(self, line):
        """
        Parse a single line of the header for PBS directives.

        Returns None if this is not a header

        Returns array of tuples with PBS options.
            The first element of the tuple is the option
            The second element is the value
        """

        # header: either start with '#' or is empty (allow whitespace)
        if not line.lstrip().startswith('#') and line.strip():
            return None

        pbsheader = self.regexp.search(line)
        if not pbsheader:
            return []

        return parse_commandline_string(pbsheader.group(1))

    def parse_header(self):
        """Parse the header (and add cmdline options too)"""
        for idx, line in enumerate(self.stdin):
            logging.info("submitfilter: original header line %s", line)
            headeropts = self.parseline(line)
            if headeropts is None:
                # keep this one line (don't strip newline)
                self.prebody = line
                break

            self.header.append(line.lstrip().rstrip("\n"))

            if headeropts:
                # last processed option wins ?
                # not so for resources
                self.allopts.extend(headeropts)
                self.occur.extend([idx] * len(headeropts))

        # extend with commandline opts
        self.allopts.extend(self.cmdlineopts)
        self.occur.extend([None] * len(self.cmdlineopts))

    def gather_state(self, master_reg):
        """
        Build a total state as defined by the options from headers and commandline

        Returns a tuple of a dict and a list:
          dict, with key/values
            - option and value
                - resource option l is treated specially
            - extras
                - _cluster : clustername
            (always contains the 'l' key).

          list with all newopts (in case they were modified)
        """

        # determine cluster for resources
        cluster = cluster_from_options(self.allopts, master_reg)

        # initialise the resource option with a dict
        state = {
            '_cluster': cluster,
            'l': {},
        }

        newopts = []
        for opt, val in self.allopts:
            if opt == 'l':
                # handle resources seperately
                newopts.append(parse_resources(val, cluster, state['l'], update=True))
            else:
                # just override ?
                state[opt] = val
                newopts.append(val)

        return state, newopts


def parse_commandline_list(args):
    """
    Parse the command line when passed as pre-split list of strings (typically sys.argv).
    Returns list of tuples (option, value)
    """
    res = []

    logging.info("submitfilter: commandline %s", " ".join(args))

    size = len(args)
    for idx, data in enumerate(args):
        if not data.startswith('-'):
            continue

        opt = data[1:]  # cut off leading -

        if (idx + 1 == size) or args[idx + 1].startswith('-'):
            val = None
        else:
            val = args[idx + 1]

        res.append((opt, val))

    return res


def parse_commandline_string(line):
    """
    Parse the command line when passed as single line string (e.g. the text after a #PBS in the header)
    Returns list of tuples (option, value)
    """
    return [reg_opt.groups() for reg_opt in PBS_OPTION_REGEXP.finditer(line)]


def parse_resources(txt, cluster, resources, update=False):
    """
    Handle any specified resources via -l option (or directive)

    Returns string with resources (which might be different from original 'txt'
    due to templating, e.g. ppn=all -> ppn=16)

    If update is True, resources will be updated with new values
    """
    newtxt = []

    # multiple resources in same txt are ',' separated
    for r in txt.split(','):
        values = r.split('=')
        key = values[0]

        try:
            value = '='.join(values[1:])
        except IndexError:
            # no '=' in resource
            value = None

        newvalue = {}
        if key == NODES_PREFIX:
            newvtxt = parse_resources_nodes(value, cluster, newvalue)
        elif MEM_REGEXP.search(key):
            newvtxt = parse_mem(key, value, cluster, newvalue)
        else:
            newvalue = {key: value}
            if value is None:
                newvtxt = key
            else:
                newvtxt = "%s=%s" % (key, value)

        if update or key not in resources:
            resources.update(newvalue)

        newtxt.append(newvtxt)

    return ','.join(newtxt)


def _parse_mem_units(txt):
    """
    Given txt, try to convert it to memory value in bytes.
    Return None if it is not possible.
    """
    r = MEM_VALUE_REG.search(txt)
    if not r:
        return None

    # TODO: also support floats
    mem = int(r.group(1))
    unit = r.group(2) or ''

    value = mem * 2**(10*MEM_VALUE_UNITS.index(unit.lower()))

    return value


def parse_mem(name, txt, cluster, resources):
    """
    Convert <name:(p|v|pv)?mem>=<txt> for cluster

    update resources instance with
        _<name>: value in bytes
        <name>: (possibly modified) resource text

    returns (possibly modified) resource text

    Supported modifications:
        (v|p)mem=all/full ; (v|p)mem=half
    """
    req_in_bytes = _parse_mem_units(txt)

    if req_in_bytes is None:
        (ppp, vpp) = get_cluster_mpp(cluster)
        maxppn = get_cluster_maxppn(cluster)

        convert = {
            PMEM: maxppn * ppp,
            VMEM: maxppn * vpp,
        }

        # multiplier 1 == identity op
        multi = lambda x: x
        if name not in (PMEM, VMEM, MEM):
            # TODO: and do what? use pmem?
            warn('Unsupported memory specification %s with value %s' % (name, txt))
        elif txt == 'half':
            multi = lambda x: int(x/2)

        # default to pmem
        req_in_bytes = multi(convert.get(name, convert[PMEM]))
        txt = "%s" % req_in_bytes

    resources.update({
        name: txt,  # original notation if possible
        "_%s" % name: req_in_bytes,
    })

    return "%s=%s" % (name, txt)


def parse_resources_nodes(txt, cluster, resources):
    """
    Convert -l nodes=<txt> for cluster

    update resources instance with
        _nrnodes: number of nodes
        _nrcores: total cores
        _nrgpus: number of GPUs
        _ppn: (avg) ppn
        _features: optional features
        nodes: (possibly modified) resource text

    returns (possibly modified) resource text

    Supported modifications:
        ppn=all/full ; ppn=half
    """
    # syntax is a +-separated node_spec
    # each node_spec has id[:prop[:prop[:...]]]
    # id is integer (=number of nodes) or a single node name
    # property: either special ppn=integer or something else/arbitrary

    logging.info("submitfilter: node resources requested %s", txt)
    maxppn = get_cluster_maxppn(cluster)

    nrnodes = 0
    nrcores = 0
    nrgpus = 0
    features = []
    newtxt = []
    for node_spec in txt.split('+'):
        props = node_spec.split(':')

        ppns = [(x.split('=')[1], idx) for idx, x in enumerate(props) if x.startswith('ppn=')] or [(1, None)]

        ppn = ppns[0][0]
        try:
            ppn = int(ppn)
        except ValueError:
            if ppn in ('all', 'full',):
                ppn = maxppn
            elif ppn == 'half':
                ppn = max(1, int(maxppn / 2))
            else:
                # it's ok to always warn for this
                # (even if it is not the final used option)
                warn("Warning: unknown ppn (%s) detected, using ppn=1" % (ppn,))
                ppn = 1

        ppntxt = 'ppn=%s' % ppn
        if ppns[0][1] is None:
            props.append(ppntxt)
        else:
            props[ppns[0][1]] = ppntxt

        try:
            nodes = int(props[0])
        except (ValueError, IndexError):
            # some description
            nodes = 1

        gpus = [x.split('=')[1] for x in props if x.startswith('gpus=')] or [0]

        features.extend([x for x in props[1:] if '=' not in x])

        nrgpus += int(gpus[0])
        nrnodes += nodes
        nrcores += nodes * ppn

        newtxt.append(':'.join(props))

    # update shared resources dict
    resources.update({
        '_nrnodes': nrnodes,
        '_nrcores': nrcores,
        '_nrgpus': nrgpus,
        '_features': features,
        '_ppn': max(1, int(nrcores / nrnodes)),
        NODES_PREFIX: '+'.join(newtxt),
    })

    return "%s=%s" % (NODES_PREFIX, resources[NODES_PREFIX])


def cluster_from_options(opts, master_reg):
    """Return the cluster based on options and/or environment"""

    queues = [val for opt, val in opts if opt == 'q']

    warntxt = []
    if queues:
        # Only consider the last one
        r = master_reg.search(queues[-1])
        if r:
            return r.group(1)
        else:
            warntxt.append('queue %s' % queues[-1])

    slurm_clusters = os.environ.get('SLURM_CLUSTERS')
    if slurm_clusters:
        try:
            return slurm_clusters.split(',')[0]
        except Exception:
            server = None
            warntxt.append('invalid SLURM_CLUSTERS %s' % slurm_clusters)
    else:
        server = os.environ.get('PBS_DEFAULT', None)

    if server:
        r = master_reg.search(server)
        if r:
            return r.group(1)
        else:
            warntxt.append('PBS_DEFAULT %s' % server)
    else:
        warntxt.append('no PBS_DEFAULT')

    features = [val for opt, val in opts if opt == 'l']
    if features:
        r = master_reg.search(' '.join(features))
        if r:
            return r.group(1)

    return DEFAULT_SERVER_CLUSTER
