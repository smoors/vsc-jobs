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
    vmemreg = re.compile('^#PBS -l .*?vmem=')
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

    #process appended results to l
    opts.vmem = None
    opts.ppn = None
    if opts.l:
        for arg in opts.l:
            for a in arg.split(','): #these options to l can be separated by a :
                if not opts.vmem and a.startswith('vmem'): #only process first occurrence
                    try:
                        opts.vmem = a.split('=')[1]
                    except: #safety for typo's on client side, will use default vmem then
                        opts.vmem = False
                        
                elif not opts.ppn and a.startswith('nodes'):
                    t = a.split('=')
                    try:
                        opts.ppn = int(t[2])
                    except:
                        opts.ppn = 1
                        
    #check for server in options
    if opts.q:
        t = serverreg.match(opts.q)
        if t:
            server = t
                
    #process stdin
    mail = None
    vmem = None
    ppn = None
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
        if not mail: #if mail not yet found
            mail = mailreg.match(line) #returns None if no match
        if not vmem:
            vmem = vmemreg.match(line)
        if not ppn:
            t = ppnreg.match(line)
            if t:
                ppn = t.group(0) #returns '' if no match, which evalutates to false
        if server == DEFAULT_SERVER:
            t = serverreg.match(line)
            if t:
                server = t.group(0)
#        else:
#            body += line 

    #combine results
    #vmem
    if not opts.vmem and not vmem:
        
        vmem = 1536 
        #try to find the server if not set yet
        if server == DEFAULT_SERVER:
            try:
                server = os.environ['PBS_DEFAULT']
            except:
                pass
            
        #compute vmem
        if 'gengar' in server or DEFAULT_SERVER in server:
            vmem = 4096 # in MB, ( 16G (RAM) + 16G (half of swap) ) / 8
        elif 'gastly' in server or 'haunter' in server:
            vmem = 2304 # in MB, ( 12G (RAM) + 6G (half of swap) ) / 8
        elif 'gulpin' in server:
            vmem =  2250 # in MB, ( 64G (RAM) + 8G (half of swap) ) / 32
        elif 'dugtrio' in server:
            vmem = None #dont set it if not found
        
        if vmem:
            #compute real vmem needed
            if opts.ppn:
                vmem = vmem * opts.ppn
            elif ppn:
                vmem = vmem * int(ppn)
            header += "# No vmem limit specified - added by submitfilter (server found: %s)\n#PBS -l vmem=%smb\n" % (server, vmem)
        
    #mail
    if not opts.m and not mail:
        header += "# No mail specified - added by submitfilter\n#PBS -m n\n"
    
    
    print header
    print body
    
    #print rest of stdin to stdout
    for line in iter(sys.stdin.readline, ''):
        sys.stdout.write(line)
    
if __name__ == '__main__':
    #testOptionParser()
    main()

