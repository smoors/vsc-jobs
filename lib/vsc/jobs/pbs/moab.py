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
A moab module to be used

@author: Stijn De Weirdt (Ghent University)
"""
# TODO proper moab namespace and tools

from lxml import etree
from vsc.utils.run import RunAsyncLoop
import vsc.jobs.pbs.nodes as pbs_nodes
from vsc.jobs.pbs.tools import str2byte
from vsc.utils import fancylogger


_log = fancylogger.getLogger('pbs.moab')

# map moab node states to pbs node states
# lowercase keys!
MOAB_PBS_NODEMAP = {
                    "busy": [pbs_nodes.ND_job_exclusive],
                    "drained": [pbs_nodes.ND_offline],  # ?
                    "down": [pbs_nodes.ND_down],
                    "idle": [pbs_nodes.ND_free, pbs_nodes.ND_idle],
                    "running": [pbs_nodes.ND_free_and_job, pbs_nodes.ND_free],
                    }


def get_nodes_dict(something=None, xml=None):
    """Similar to derived getnodes from vsc.pbs.interface.get_nodes_dict

    returns a dict of nodes, with a 'status' field which is a dict of statusses
    the something parameter is ignored. (for now)
    """
    if xml is None:
        cmd = "mdiag -n --format=xml"
        err, xml = RunAsyncLoop.run(cmd.split())
        if err:
            _log.error("Problem occurred running %s: %s (%s)" % (cmd, err, xml))
            return None

    # build tree
    tree = etree.fromstring(xml)
    nodes = {}
    for node in tree:
        # <node AVLCLASS="[bshort][debug][short][long][special][workshop]"
        # CFGCLASS="[bshort][debug][short][long][special][workshop]"
        # FEATURES="hadoop,collectl" FLAGS="rmdetected" JOBLIST="3956525"
        # LASTUPDATETIME="1363206409" LOAD="8.160000" MAXJOB="0"
        # MAXJOBPERUSER="0" MAXLOAD="0.000000" NODEID="node001.gengar.gent.vsc"
        # NODEINDEX="1" NODESTATE="Busy" OS="linux" OSLIST="linux" PARTITION="gengar"
        # PRIORITY="0" PROCSPEED="0" RADISK="92194" RAMEM="16053" RAPROC="0" RASWAP="34219"
        # RCDISK="92381" RCMEM="16053" RCPROC="8" RCSWAP="36533" RESCOUNT="1"
        # RMACCESSLIST="gengar" RSVLIST="3956525" SPEED="1.000000" STATACTIVETIME="24357970"
        # STATMODIFYTIME="1363076905" STATTOTALTIME="25499884" STATUPTIME="24971920">
        host = node.get("NODEID")
        nodes[host] = {}
        nodes[host]['xml'] = node.items()
        states = MOAB_PBS_NODEMAP[node.get("NODESTATE").lower()]
        derived = {
                   'states': states,
                   'state': states[0],
                   'size': str2byte(node.get("RCDISK") + "mb"),
                   'physmem': str2byte(node.get("RCMEM") + "mb"),
                   'np': int(node.get("RCPROC")),
                   }
        # add state mapping to derived
        pbs_nodes.make_state_map(derived)

        nodes[host]['derived'] = derived

    return nodes


def showstats(xml=None):
    """Return a dict of the showstats command"""
    if xml is None:
        cmd = "showstats --xml"
        err, xml = RunAsyncLoop.run(cmd.split())
        if err:
            _log.error("Problem occurred running %s: %s (%s)" % (cmd, err, xml))
            return None

    # [root@master2 ~]# showstats && showstats --xml
    #
    # moab active for    1:13:38:20  stats initialized on Wed May 16 12:25:45 2012
    #
    # Eligible/Idle Jobs:              2159/5473   (39.448%)
    # Active Jobs:                        5
    # Successful/Completed Jobs:     742474/742474 (100.000%)
    # Avg/Max QTime (Hours):           8.27/435.18
    # Avg/Max XFactor:                 0.01/33072.70
    #
    # Dedicated/Total ProcHours:      7.63M/9.66M  (78.986%)
    #
    # Current Active/Total Procs:       280/1240   (22.581%)
    #
    # Avg WallClock Accuracy:          34.190%
    # Avg Job Proc Efficiency:         53.589%
    # Est/Avg Backlog:              2:07:20:04/5:04:03:37
    #
    #
    # <Data><stats ABP="36.81" AQT="29768.83" AXF="0.01" Duration="82" GCAJobs="5" GCEJobs="2159"
    # GCIJobs="5473" GPHAvl="9663746.17" GPHDed="7632994.73" GPHSuc="774590033.19" GPHUtl="7786240.72"
    # JStartRate="0.000000" JSubmitRate="12.375721" JSuccessRate="0.057676" MBP="21925" MQT="1566652"
    # MXF="33072.70" MinEffIteration="6" SpecDuration="1800" StartTime="1363076571" TEvalJC="297839963"
    # TJA="253854.97" TJC="742474" TMSA="69828097826144.70" TMSD="2281436455568.04" TMSU="3681916256.18"
    # TNJA="19135819284.54" TNJC="1515171" TPSD="27891042026.64" TPSE="27885241195.00" TPSR="130540048087.00"
    # TPSU="14946402091.95" TSchedDuration="13550031" TStartJC="971878" TStartPC="2520118"
    # TStartQT="56002118567" TStartXF="3440289.00" TSubmitJC="984840" TSubmitPH="55171206.00"
    # ThroughputTime="188978568306"></stats>
    # <sys APS="32772960" ATAPH="20191" ATQPH="133644" IC="7126" IMEM="2488215" INC="120" IPC="960"
    # QPS="537871292" RMPI="20" SCJC="742474" UPMEM="2488215" UPN="155" UPP="1240" statInitTime="1337163945"
    # time="1363212082"></sys></Data>

    # fresh moab gives
    # <Data><stats Duration="1380095491" GPHAvl="4900.22" MinEffIteration="6" SpecDuration="1800"
    # StartTime="1380059385" TMSA="71111193148.80" TSchedDuration="3430048"></stats><sys ATAPH="0"
    # ATQPH="0" IC="1701" IMEM="2063904" INC="32" IPC="512" RMPI="20" UPMEM="2063904" UPN="32" UPP="512"
    # statInitTime="1380059220" time="1380093691"></sys></Data>

    # build tree
    # GPHDed is missing with initial moab restart, would trigger KeyError later on
    res = {'stats':{'GPHDed': 0.0, }}
    tree = etree.fromstring(xml)
    for el in  tree.getchildren():
        elres = res.setdefault(el.tag, {})
        for k, v in el.items():
            try:
                v = int(v)
            except:
                try:
                    v = float(v)
                except:
                    pass
            elres[k] = v

    upp = res['sys'].get('UPP', 0)
    ipc = res['sys'].get('IPC',0)
    if upp:
        ste = 100.0 * (1.0 - 1.0 * ipc / upp)
    else:
        ste = 0
    summary = {
               'DPH': res['stats']['GPHDed'],  # Dedicated ProcHours
               'TPH': res['stats']['GPHAvl'],  # Total ProcHours
               'LTE': 100.0 * (res['stats']['GPHDed'] / res['stats']['GPHAvl']),  # LongTerm Efficiency in %
               'CAP': upp - ipc,  # Current Active Procs
               'CTP': upp,  # Current Total Procs
               'STE': upp,  # ShortTerm Efficiency in %
               }

    res['summary'] = summary
    return res

