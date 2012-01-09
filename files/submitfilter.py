#!/usr/bin/env python
"""
@author: Jens Timmerman

Filters scripts submitted by qsub,
adds default values and command line parameters to the script
for processing by pbs
"""

#-mail options parsen
#-vmem option parsen
#-cores parsen (voor vmem)
from optparse import OptionParser, BadOptionError
import optparse
import sys
import re
import os

DEFAULT_SERVER = "default"
GENGAR_VMEM = 4096
GENGAR_VMEM_WARNING = 47313 #more then this and the job won't start
GASTLY_VMEM = 2304
GASTLY_VMEM_WARNING = 23712 #same for haunter
GULPIN_VMEM = 2250
GULPIN_VMEM_WARNING = 80948
DUGTRIO_VMEM_WARNING = 1395439 #total amount of ram in dugtrio cluster
class PassThroughOptionParser(OptionParser):
    """
    "Pass-through" option parsing -- an OptionParser that ignores
    unknown options and lets them pile up in the leftover argument
    list.  Useful for programs that pass unknown options through
    to a sub-program.
    from http://www.koders.com/python/fid9DFF5006AF4F52BA6483C4F654E26E6A20DBC73C.aspx?s=add+one#L27
    """
    def __init__(self):
        OptionParser.__init__(self,add_help_option=False,usage=optparse.SUPPRESS_USAGE)
        
    def _process_long_opt(self, rargs, values):

        try:
            OptionParser._process_long_opt(self, rargs, values)
        except BadOptionError, err:
            self.largs.append(err.opt_str)

    def _process_short_opts(self, rargs, values):
        try:
            OptionParser._process_short_opts(self, rargs, values)
        except BadOptionError, err:
            self.largs.append(err.opt_str)


def main(arguments=sys.argv):
    """
    main method
    """
    #regexes needed here
    mailreg = re.compile("^#PBS\s+-m\s.+")
    vmemreg = re.compile('^#PBS\s+-l\s+[^#]*?vmem=(?P<vmem>[^\s]*)')
    ppnreg = re.compile('^#PBS\s+-l\s+[^#]*?nodes=.+?:ppn=(?P<ppn>\d+)')
    serverreg = re.compile('.*@master[0-9]*\.(?P<server>[^.]*)\.gent\.vsc')
    #optsppnreg = re.compile('nodes=(?P<nodes>\d+)[^:#,\s]ppn=(?P<ppn>\d+)')
    optsppnreg = re.compile('.*?nodes=(?P<nodes>\d+)[^#,\s]*ppn=(?P<ppn>\d+)')

    optsvmemreg = re.compile('vmem=(?P<vmem>[^#\s]*)')
    #parse command line options
    parser = PassThroughOptionParser() #ignore unknown options
    parser.add_option("-m", help="mail option")
    parser.add_option("-q", help="queue/server option")
#    parser.add_option("-h", help="dummy option to prevent printing help",action="count")
    parser.add_option("-l", help="some other options", action="append")
    
    #parser.add_option()
    (opts, args) = parser.parse_args(arguments)
#    print "options:", opts
#    print "args:", args
    vmemDetected =False
    ppnDetected = False
    mailDetected = bool(opts.m)
    serverDetected = False
    opts.server = None
    #process appended results to l
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

                        
    #check for server in options
    if opts.q:
        t = serverreg.match(opts.q)
        if t:
            opts.server = t
            serverDetected = True
                
    #process stdin
    header = ""
    body = ""
    for line in iter(sys.stdin.readline, ''):
        #check if we're still in the preamble
        if not line.startswith('#') and not line.strip() == "":
            #jump out of loop here, we will print the rest later
            body = line #save this line first
            break 
        header += line 
        #check if this line is mail
        if not mailDetected: #if mail not yet found
            opts.m = mailreg.match(line) #returns None if no match
            mailDetected = bool(opts.m)
        if not vmemDetected:
            opts.vmem = vmemreg.match(line)
            vmemDetected = bool(opts.vmem)
            if vmemDetected:
                opts.vmem = opts.vmem.group("vmem")
        if not ppnDetected:
            t = ppnreg.match(line)
            if t:
                opts.ppn = t.group("ppn") #returns '' if no match, which evalutates to false
                ppnDetected = bool(opts.ppn)
        if not serverDetected:
            t = serverreg.match(line)
            if t:
                opts.server = t.group("server")
                serverDetected = True
                

    #combine results
    #vmem
        
    #try to find the server if not set yet
    if not serverDetected and os.environ.has_key('PBS_DEFAULT'):
        opts.server = os.environ['PBS_DEFAULT']
        serverDetected = True
        
    # check whether VSC_NODE_PARTITION environment variable is set
    # used for gulpin/dugtrio
    
    if os.environ.has_key('VSC_NODE_PARTITION'):
        header += "\n#PBS -W x=PARTITION:%s\n" % os.environ['VSC_NODE_PARTITION']
        
    #set defaults
    if not serverDetected:
        opts.server = DEFAULT_SERVER
    if not ppnDetected:
        opts.ppn = 1
        
    
    #compute vmem
    if not serverDetected or re.search("\.gengar\.", opts.server):
        tvmem = GENGAR_VMEM # in MB, ( 16G (RAM) + 16G (half of swap) ) / 8
        maxvmem =GENGAR_VMEM_WARNING
    elif re.search("\.(gastly|haunter)\.", opts.server):
        tvmem = GASTLY_VMEM # in MB, ( 12G (RAM) + 6G (half of swap) ) / 8
        maxvmem =GASTLY_VMEM_WARNING
    elif re.search("\.gulpin\.", opts.server):
        tvmem =  GULPIN_VMEM # in MB, ( 64G (RAM) + 8G (half of swap) ) / 32
        maxvmem =GULPIN_VMEM_WARNING
    elif re.search("\.dugtrio\.", opts.server):
        tvmem = None #dont set it if not found
        maxvmem = DUGTRIO_VMEM_WARNING
        vmemDetected = True
    else:
        # backup, but should not occur
        tvmem = 1536
        maxvmem = 0
        sys.stderr.write("Warning: unknown server (%s) detected, see PBS_DEFAULT. This should not be happening...\n"%opts.server)
    
    if not vmemDetected:
        #compute real vmem needed
        vmem = tvmem * int(opts.ppn)
        header += "# No vmem limit specified - added by submitfilter (server found: %s)\n#PBS -l vmem=%smb\n" % (opts.server, vmem)
    else:
        #parse detected vmem to check if to much was asked
        groupvmem = re.search('(\d+)', opts.vmem).group(1)
        if groupvmem:
            intvmem = int(groupvmem)
        else:
            intvmem = 0
        if opts.vmem.endswith('gb'):
            intvmem = intvmem * 1024
        if intvmem > maxvmem: 
            #warn user that he's trying to request to much vmem
            sys.stderr.write("Warning, requested %sMB vmem per node, this is more then the available vmem (%sMB), this job will never start.\n" % (intvmem,maxvmem))
    #mail
    if not mailDetected:
        header += "# No mail specified - added by submitfilter\n#PBS -m n\n"
    
    
    print header
    print body
    
    #print rest of stdin to stdout
    for line in iter(sys.stdin.readline, ''):
        sys.stdout.write(line)
    #print ("#done")
    
if __name__ == '__main__':
    #testOptionParser()
    main()

