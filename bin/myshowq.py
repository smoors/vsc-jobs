#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2009-2013 Ghent University
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
@author Andy Georges
"""

import cPickle
import os
import pwd
import sys
import time

from vsc.utils import fancylogger
from vsc.utils.generaloption import simple_option

logger = fancylogger.getLogger("myshowq")
fancylogger.setLogLevelDebug()
fancylogger.logToScreen(True)

maxage = 60 * 30  # 30 minutes


def readbuffer(owner, showvo, running, idle, blocked, location=None):
    """
    Unpickle the file and fill in the resulting datastructure.
    """

    dest = None
    if location:
        dest = os.path.join(os.getenv(location), ".showq.pickle")

    if not dest:
        home = pwd.getpwnam(owner)[5]

        if not os.path.isdir(home):
            print "Homedir %s for owner %s not found" % (home, owner)
            return (None, None)
        dest = "%s/.showq.pickle" % home

    logger.debug("destination, well, source, suh is %s" % (dest))

    try:
        f = open(dest)
        (res, user_map) = cPickle.load(f)
        f.close()
    except Exception, err:
        print "Failed to load pickle from file %s: %s" % (dest, err)
        return (None, None)

    if not 'timeinfo' in res:
        print "No timeinfo found in res: %s" % err
        return (None, None)

    ## check for timeinfo
    if res['timeinfo'] < (time.time() - maxage):
        print "outdated"
        return (None, None)
    else:
        del res['timeinfo']

    print res

    # Filter out data that is not needed
    if not showvo:
        for user in res.keys():
            if not user == owner:
                #del res[user]
                pass

    for user in res.keys():
        for host in res[user].keys():
            print "looking at host %s" % (host)
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

    return (res,user_map)


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

def showdetail(res,user_map,owner,showvo):
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
    print "Not implemented, see source code for info to implement this."


def showsummary(res,user_map,owner,showvo):
    """
    Show summary info
    -- owner first if possible
    """
    summUserHosts={}
    summaryUsers={} # summary per user
    summaryHosts={} # summary per host
    summ=[0,0,0,0,0,0,0,0] # overall summary
    for us in res.keys():
        # total for this user (jobs running, jobs idle, jobs blocked,  total jobs, cpus running, cpus idle, cpus blocked, cpus total)
        ru=[0,0,0,0,0,0,0,0]
        if not summUserHosts.has_key(us):
            summUserHosts[us] = {}
        for host in res[us].keys():
            if not summaryHosts.has_key(host):
                summaryHosts[host]=[0,0,0,0,0,0,0,0]
            if not summUserHosts[us].has_key(host):
                r = [0,0,0,0,0,0,0,0]
            for st in res[us][host].keys():
                for j in res[us][host][st]:
                    if st in ('Running'):
                        r[0]+=1
                        r[4]+=int(j['ReqProcs'])
                    else:
                        ## all idle, also blocked jobs
                        r[1]+=1
                        r[5]+=int(j['ReqProcs'])
                        if not st in ('Running','Idle'):
                            r[2]+=1
                            r[6]+=int(j['ReqProcs'])
            r[3]=r[0]+r[1]
            r[7]=r[4]+r[5]
            summUserHosts[us][host]=tuple(r)
            for x in xrange(len(ru)):
                ru[x]+=r[x]
            for x in xrange(len(summaryHosts[host])):
                summaryHosts[host][x]+=r[x]
        summaryUsers[us]=tuple(ru)
        for x in xrange(len(summ)):
            summ[x]+=ru[x]
    summ=tuple(summ)

    users=res.keys()
    users.sort()
    if owner in users:
        users.remove(owner)
        users.insert(0,owner)

    usernames=[]
    for user in users:
        usernames.append(user_map[user])
    #usernames=makemap(users,owner)

    totalStr="TOTAL"
    overallStr="OVERALL"

    hosts=["gengar","gastly","haunter","gulpin","dugtrio","raichu"]

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

    tmp='%'+str(maxint)+'i'
    templ=tmp*8

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
        "summary": ("Give the summary", None, "store_true", False, 's'),
        "detail": ("Detailed information", None, "store_true", False,),
        "virtualorganisation": ("Give VO details if available", None, "store_true", False, 'v'),
        "running": ("Display running job information", None, "store_true", False, 'r'),
        "idle": ("Display idle job information", None, "store_true", False, 'i'),
        "blocked": ("Dispay blocked job information", None, "store_true", False, 'b'),
        'location_environment': ('the location for storing the pickle file depending on the cluster', str, 'store', 'VSC_HOME'),
    }

    opts = simple_option(options)

    if not (opts.options.running or opts.options.idle or opts.options.blocked):
        opts.options.running = True
        opts.options.idle = True
        opts.options.blocked = True

    my_uid = os.geteuid()
    my_name = pwd.getpwuid(my_uid)[0]

    (res, user_map) = readbuffer(my_name,
                                 opts.options.virtualorganisation,
                                 opts.options.running,
                                 opts.options.idle,
                                 opts.options.blocked,
                                 opts.options.location_environment)

    if not res or len(res) == 0:
        print "no data"
        sys.exit(0)

    if opts.options.summary:
        showsummary(res, user_map, my_name, opts.options.virtualorganisation)
    if opts.options.detail:
        showdetail(res, user_map, my_name, opts.options.virtualorganisation)


if __name__ == '__main__':
    main()
