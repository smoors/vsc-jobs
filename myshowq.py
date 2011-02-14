#!/usr/bin/python

"""
Read showq pickle from $HOME/.showq.pickle
- if outdated (> 1h or so), ignore (ie no jobs)
- all sorts of format options
-- VO
-- summary
-- detail
-- running/idle/blocked

- add translation of account to realname?
-- forsee interface (autodetect special file)

"""
maxage=60*15

import sys,os,re,time,pwd,cPickle

def usage():
    print """
myshowq options:
    
    -s    Summary (default)
    -d    Detailed info
    
    -v    VO info (default: only show own jobs)
    
    -r    Running jobs
    -i    Idle jobs
    -b    Blocked jobs
    
    -h    Print this usage info
"""

def readbuffer(owner,showvo,running,idle,blocked):    
    """
    cpickle file to res. 
    """

    home=pwd.getpwnam(owner)[5]
    if not os.path.isdir(home):
        print "Homedir %s owner %s not found"%(home, owner)
        return (None,None)
    
    dest="%s/.showq.pickle"%home
    try:
        f=open(dest)
        (res,userMap)=cPickle.load(f)
        f.close()
    except Exception ,err:
        print "Failed to load pickle from file %s: %s"%(dest,err)
        return (None,None)
    
    try:
        res.has_key('timeinfo')
    except Exception,err:
        ## old format. no print since irrelevant for user
        #print "No timeinfo found in res: %s"%err
        return (None,None)
    
    ## check for timeinfo
    if not res.has_key('timeinfo') or res['timeinfo'] < (time.time() - maxage):
        return (None,None)
    else:
        del res['timeinfo']
    

    """
    Filter out data that is not needed
    """
    if not showvo:
        for us in res.keys():
            if not us == owner:
                del res[us]

    for us in res.keys():
        for host in res[us].keys():
            states=res[us][host].keys()
            if not running:
                if 'Running' in states:
                    del res[us][host]['Running']
            if not idle:
                if 'Idle' in states:
                    del res[us][host]['Idle']
            if not blocked:
                for st in [x for x in states if not x in ('Running','Idle')]:
                    del res[us][host][st]
        
    return (res,userMap)

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

def showdetail(res,userMap,owner,showvo):
    """
    Show detailed info 
    
    Fill in your own implementation here, by processing the data as you please
    
    'res' contains all job info for all users in the VO
    i.e. a dictionary of users to hosts (gengar,gastly,haunter) to jobs (see also showSummary)
    
    available info for all jobs: 'ReqProcs','SubmissionTime','JobID','DRMJID','Class'
    available info for running jobs: 'MasterHost'
    available info for blocked jobs: 'BlockReason','Description'
    
    'userMap' is a dictionary of user ids to real names
    """
    print "Not implemented, see source code for info to implement this."

def showsummary(res,userMap,owner,showvo):
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
        usernames.append(userMap[user])
    #usernames=makemap(users,owner)
    
    totalStr="TOTAL"
    overallStr="OVERALL"
    
    hosts=["gengar","gastly","haunter"]
    
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


if __name__ == '__main__':
    """
    Collect all info
    """
    import getopt
    allopts = ["help"]

    running=False
    idle=False
    blocked=False
    
    showvo=False
    summary=True
    
    try:
        opts,args = getopt.getopt(sys.argv[1:], "hvribsd", allopts)
    except getopt.GetoptError,err:
        print "\n" + str(err)
        usage()
        sys.exit(2)
    
    for key, value in opts:
        if key in ("-h", "--help"):
            usage()
            sys.exit(0)
        if key in ("-v"):
            showvo=True
        if key in ("-s"):
            summary=True
        if key in ("-d"):
            summary=False
        if key in ("-r"):
            running=True
        if key in ("-i"):
            idle=True
        if key in ("-b"):
            blocked=True

    if not (running or idle or blocked):
        running=True
        idle=True
        blocked=True

    myuid=os.geteuid()
    myname=pwd.getpwuid(myuid)[0]
        
    (res,userMap)=readbuffer(myname,showvo,running,idle,blocked)
    if not res or len(res) == 0:
        sys.exit(0)
    
    if summary:
        showsummary(res,userMap,myname,showvo)
    else:
        showdetail(res,userMap,myname,showvo)
