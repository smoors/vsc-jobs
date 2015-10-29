#!/usr/bin/python
# -*- coding: latin-1 -*-
##
# Copyright 2009-2015 Ghent University
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
Displays the showq pickle file contents.

If the pickle file is outdated (older then 1 hour), no jobs are shown.

Several formating options are provided:
    - Show the VO information or projects information (if available on the cluster)
    - Provide a summary
    - Provide details
        - Running jobs
        - Idle jobs
        - Blocked jobs

@author Stijn De Weirdt
@author Wouter Depypere
@author Andy Georges
"""

import copy
import os
import sys
import time

from pwd import getpwuid
from vsc.config.base import VscStorage
from vsc.utils import fancylogger
from vsc.utils.cache import FileCache
from vsc.utils.generaloption import simple_option

logger = fancylogger.getLogger("myshowq")
fancylogger.setLogLevelWarning()
fancylogger.logToScreen(True)

MAXIMAL_AGE = 60 * 30  # 30 minutes


def read_cache(owner, showvo, running, idle, blocked, path):
    """
    Unpickle the file and fill in the resulting datastructure.
    """

    try:
        cache = FileCache(path)
    except:
        print "Failed to load showq information from %s" % (path,)

    res = cache.load('showq')[1][0]
    user_map = cache.load('showq')[1][1]
    ## check for timeinfo
    if res['timeinfo'] < (time.time() - MAXIMAL_AGE):
        print "The data in the showq cache may be outdated. Please contact your admin to look into this."
    #    return (None, None)

    del res['timeinfo']

    logger.debug("Resulting cache data: %s" % (res))

    # Filter out data that is not needed
    if not showvo:
        for user in res.keys():
            if not user == owner:
                #del res[user]
                pass

    for user in res.keys():
        for host in res[user].keys():
            logger.debug("looking at host %s" % (host))
            states = res[user][host].keys()
            if not running:
                if 'Running' in states:
                    del res[user][host]['Running']
            if not idle:
                if 'Idle' in states:
                    del res[user][host]['Idle']
            if not blocked:
                for state in [x for x in states if not x in ('Running','Idle')]:
                    del res[user][host][state]

    return (res, user_map)


def makemap(users,owner):
    """
    Check for user map
    - map is a python file with a dictionary called map
    """
    newusers=users
    home=pwd.getpwnam(owner)[5]
    dest="%s/.showq.pickle.map"%home

    if os.path.isfile(dest):
        try:
            execfile(dest)
        except Exception, err:
            print "Failed to load map file %s: %s"%(dest,err)
        if locals().has_key("map"):
            mymap=locals()['map']
            try:
                newusers=[mymap.get(us,us) for us in users]
            except Exception, err:
                print "Failed to make mapping: %s"%err
        else:
            print "Map loaded, but no map dictionary found."

    return newusers


def showdetail(hosts, res, user_map, owner, showvo):
    """
    Show detailed info

    Fill in your own implementation here, by processing the data as you please

    'res' contains all job info for all users in the VO
    i.e. a dictionary of users to hosts (gengar,gastly,haunter) to jobs (see also showSummary)

    available info for all jobs: 'ReqProcs','SubmissionTime','JobID','DRMJID','Class'
    available info for running jobs: 'MasterHost'
    available info for blocked jobs: 'BlockReason','Description'

    'user_map' is a dictionary of user ids to real names
    """
    print "This functionality is not implemented yet. Please check the myshowq command source code for pointers on how to implement this."


def showsummary(hosts, res, user_map, owner, showvo):
    """
    Show summary info
    -- owner first if possible
    """

    job_data = {
        'jobs running': 0,
        'jobs idle': 0,
        'jobs blocked': 0,
        'jobs total': 0,
        'cpus running': 0,
        'cpus idle': 0,
        'cpus blocked': 0,
        'cpus total': 0,
    }

    summ = copy.deepcopy(job_data)
    summUserHosts = {}
    summaryUsers = {} # summary per user
    summaryHosts = {} # summary per host

    for us in res.keys():

        summary_user = copy.deepcopy(job_data)

        if not summUserHosts.has_key(us):
            summUserHosts[us] = {}

        for host in res[us].keys():
            if not summaryHosts.has_key(host):
                summaryHosts[host] = copy.deepcopy(job_data)

            if not summUserHosts[us].has_key(host):
                summary = copy.deepcopy(job_data)

            for state in res[us][host].keys():
                for j in res[us][host][state]:
                    if state in ('Running'):
                        summary['jobs running'] += 1
                        summary['cpus running'] += int(j['ReqProcs'])
                    else:
                        ## all idle, also Blocked jobs
                        summary['jobs idle'] += 1
                        summary['cpus idle'] += int(j['ReqProcs'])
                        if not state in ('Running', 'Idle'):
                            summary['jobs blocked'] += 1
                            summary['cpus blocked'] += int(j['ReqProcs'])
            summary['jobs total'] = summary['jobs running']+summary['jobs idle']
            summary['cpus total'] = summary['cpus running']+summary['cpus idle']
            summUserHosts[us][host] = summary

            for k in summary_user.keys():
                summary_user[k] += summary[k]

            for k in summaryHosts[host].keys():
                summaryHosts[host][k] += summary[k]

        summaryUsers[us] = summary_user

        for k in summ.keys():
            summ[k] += summary_user[k]

    users = res.keys()
    users.sort()
    if owner in users:
        users.remove(owner)
        users.insert(0,owner)

    usernames = []
    for user in users:
        usernames.append(user_map[user])
    #usernames=makemap(users,owner)

    totalStr="TOTAL"
    overallStr="OVERALL"

    ## maximum namelength + extra whitespace
    maxlen=max([len(x) for x in usernames])+2
    ## maximum hostname length + extra characters
    maxlenhost=max([len(x) for x in (hosts+[totalStr,overallStr])])+3
    ## maximum display size for integers (job/procs) with extra whitespace
    maxint=9+2
    tmp='%'+str(maxint)+'s'
    templ=tmp*4
    rit=templ%('Run','Idle','(Blocked)','Total')
    lrit=(len(rit)-4+1)/2

    # dirty :)
    tmp='%%%s'+str(maxint)+'i'
    templ=(tmp * 8) % ('(jobs running)',
                       '(jobs idle)',
                       '(jobs blocked)',
                       '(jobs total)',
                       '(cpus running)',
                       '(cpus idle)',
                       '(cpus blocked)',
                       '(cpus total)')

    padding=''
    if showvo:
        padding='\t'

    header="%s%s%s\n"%(padding,' '*maxlenhost,'%sJobs%sCPUs%s'%(' '*lrit,' '*2*lrit,' '*lrit))
    headerlen=len(header)+5-1 # +8 for tab
    header+="%s%s%s\n"%(padding,' '*maxlenhost,rit*2)
    header+="%s%s\n"%(padding,'-'*headerlen)

    txt=''
    for us,usn in zip(users,usernames):
        if showvo:
            txt+="%s (%s)\n"%(us,usn)
        for host in hosts:
            if host in summUserHosts[us].keys():
                txt+="%s%s%s\n"%(padding,host+' '*(maxlenhost-len(host)),templ%summUserHosts[us][host])
        txt+="%s%s\n"%(padding,'~'*(maxlenhost+len(templ%summ)))
        txt+="%s%s%s\n\n"%(padding,totalStr+' '*(maxlenhost-len(totalStr)),templ%summaryUsers[us])

    footer=''
    if len(users) > 1:
        footer="%s\n"%('-'*headerlen)
        footer+="SUMMARY\n"
        for host in hosts:
            if host in summaryHosts.keys():
                footer+="%s%s%s\n"%(padding,host+' '*(maxlenhost-len(host)),templ%tuple(summaryHosts[host]))
        footer+="%s%s\n"%(padding,'~'*(maxlenhost+len(templ%summ)))
        footer+="%s%s%s\n"%(padding,overallStr+' '*(maxlenhost-len(overallStr)),templ%summ)

    print header+txt+footer


def main():
    """Yeah, so, erm. The main function and such."""

    options = {
        "summary": ("Give the summary", None, "store_true", True, 's'),
        "detail": ("Detailed information", None, "store_true", False,),
        "virtualorganisation": ("Give VO details if available", None, "store_true", False, 'v'),
        "running": ("Display running job information", None, "store_true", False, 'r'),
        "idle": ("Display idle job information", None, "store_true", False, 'i'),
        "blocked": ("Dispay blocked job information", None, "store_true", False, 'b'),
        'hosts': ("Hosts/clusters to check", None, 'extend', []),
        'location_environment': ('the location for storing the pickle file depending on the cluster', str, 'store', 'VSC_SCRATCH_DELCATTY'),
    }

    opts = simple_option(options, config_files=['/etc/myshowq.conf'])

    if not (opts.options.running or opts.options.idle or opts.options.blocked):
        opts.options.running = True
        opts.options.idle = True
        opts.options.blocked = True

    storage = VscStorage()
    user_name = getpwuid(os.getuid())[0]
    now = time.time()

    mount_point = storage[opts.options.location_environment].login_mount_point
    path_template = storage.path_templates[opts.options.location_environment]['user']
    path = os.path.join(mount_point, path_template[0], path_template[1](user_name), ".showq.json.gz")

    (res, user_map) = read_cache(user_name,
                                 opts.options.virtualorganisation,
                                 opts.options.running,
                                 opts.options.idle,
                                 opts.options.blocked,
                                 path)

    if not res or len(res) == 0:
        print "no data"
        sys.exit(0)

    if opts.options.summary:
        showsummary(opts.options.hosts, res, user_map, user_name, opts.options.virtualorganisation)
    if opts.options.detail:
        showdetail(opts.options.hosts, res, user_map, user_name, opts.options.virtualorganisation)


if __name__ == '__main__':
    main()
