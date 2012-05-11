#!/usr/bin/env python
from lxml import etree
import sys
from vsc.utils.run import Command


class Query(object):
    """
    Implements something similar to PBSQuery
    """

    def __init__(self, state=None):
        """constructor"""

    def getnodes(self):
        """Similar to getnodes from PBSQuery
        
        returns a dict of nodes, with a 'status' field which is a dict of statusses
        """
        input, err = Command("mdiag -n --format=xml").run()
        if err:
            print "Error occured running mdiag -n: %s" % err
        #build tree
        tree = etree.fromstring(input)
        nodes = {}
        for node in tree:
            nodes[node.get("NODEID")] = {'status' : {'state': node.get("NODESTATE"), 'size':node.get("RAMEM")}}
        #TODO: sort
        print nodes
