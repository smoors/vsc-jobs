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

DEFAULT_SERVER = "default"
GENGAR_VMEM = 4096
GASTLY_VMEM = 2304
GULPIN_VMEM = 2250
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
    mailreg = re.compile("^#PBS -m .+")
    vmemreg = re.compile('^#PBS -l .*?vmem=(.*)')
    ppnreg = re.compile('^#PBS -l .*?nodes=.+?:ppn=(\d+)')
    serverreg = re.compile('.*@master[0-9]*\.([^.]*)\.gent\.vsc')
    
    #parse command line options
    parser = PassThroughOptionParser() #ignore unknown options
    parser.add_option("-m", help="mail option")
    parser.add_option("-q", help="queue/server option")
#    parser.add_option("-h", help="dummy option to prevent printing help",action="count")
    parser.add_option("-l", help="some other options", action="append")

    server = DEFAULT_SERVER
    
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
    opts.ppn = None
    if opts.l:
        for arg in opts.l:
            for a in arg.split(','): #these options to l can be separated by a :
                if not vmemDetected and a.startswith('vmem'): #only process first occurrence
                    try:
                        opts.vmem = a.split('=')[1]
                        vmemDetected = True
                    except: #safety for typo's on client side, will use default vmem then
                        pass
                        
                elif not ppnDetected and a.startswith('nodes'):
                    t = a.split('=')
                    ppnDetected = True #we found the nodes specifier, so should be set now anyhow
                    try:
                        opts.ppn = int(t[2])
                    except:
                        opts.ppn = 1
                        
    #check for server in options
    if opts.q:
        t = serverreg.match(opts.q)
        if t:
            opts.server = t
            serverDetected = True
                
    #process stdin
    preamble = True
    header = ""
    body = ""
    for line in iter(sys.stdin.readline, ''):
        #check if we're still in the preamble
        if not line.startswith('#') and not line.strip() == "":
            #preamble = False
            #jump out of loop here, we will print the rest later
            body = line #save this line first
            break 
        #if preamble:
        header += line 
        #check if this line is mail
        if not mailDetected: #if mail not yet found
            opts.m = mailreg.match(line) #returns None if no match
            mailDetected = bool(opts.m)
        if not vmemDetected:
            opts.vmem = vmemreg.match(line)
            vmemDetected = bool(opts.vmem)
            if vmemDetected:
                opts.vmem = opts.vmem.group(1)
        if not ppnDetected:
            t = ppnreg.match(line)
            if t:
                opts.ppn = t.group(0) #returns '' if no match, which evalutates to false
                ppnDetected = bool(opts.ppn)
        if not serverDetected:
            t = serverreg.match(line)
            if t:
                opts.server = t.group(0)
                serverDetected = True
                
#        else:
#            body += line 

    #combine results
    #vmem
        
    tvmem = 1536 
    #try to find the server if not set yet
    if not serverDetected:
        try:
            opts.server = os.environ['PBS_DEFAULT']
            serverDetected = bool(opts.server)
        except:
            pass
        
    #set defaults
    if not serverDetected:
        opts.server = DEFAULT_SERVER
    if not ppnDetected:
        opts.ppn = 1
        
    #compute vmem
    if not serverDetected or 'gengar' in opts.server:
        tvmem = GENGAR_VMEM # in MB, ( 16G (RAM) + 16G (half of swap) ) / 8
    elif 'gastly' in opts.server or 'haunter' in opts.server:
        tvmem = GASTLY_VMEM # in MB, ( 12G (RAM) + 6G (half of swap) ) / 8
    elif 'gulpin' in opts.server:
        tvmem =  GULPIN_VMEM # in MB, ( 64G (RAM) + 8G (half of swap) ) / 32
    elif 'dugtrio' in opts.server:
        tvmem = None #dont set it if not found
        vmemDetected = True
    
    if not vmemDetected:
        #compute real vmem needed
        vmem = tvmem * int(opts.ppn)
        header += "# No vmem limit specified - added by submitfilter (server found: %s)\n#PBS -l vmem=%smb\n" % (server, vmem)
    else:
        groupvmem = re.search('(\d+)', opts.vmem).group(1)
        if groupvmem:
            intvmem = int(groupvmem)
        else:
            intvmem = 0
        if opts.vmem.endswith('gb'):
            intvmem = intvmem * 1024
        if intvmem > tvmem: #TODO: convert to gb sometimes 
            #warn user that he's trying to request to much vmem
            sys.stderr.write("Warning, requesting %s vmem, this is more then the default (%s)" % (intvmem,tvmem))
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

