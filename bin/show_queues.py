#!/usr/bin/python
"""
Prints the enabled and route queues
"""
from vsc.jobs.pbs.interface import get_queues_dict


def main():
    """Main function"""
    queues_dict = get_queues_dict()

    indent = " " * 4

    txt = []
    for name, queue in queues_dict['enabled']:
        txt.append(name)
        txt.append("%swalltime %s (max %s)" % (indent, queue['resources_default']['walltime'][0], queue['resources_max']['walltime'][0]))
    for name, queue in queues_dict['route']:
        txt.append(name)
        txt.append("%sroutes %s" % (indent, queue['route_destinations'][0]))

    print "\n".join(txt)

if __name__ == "__main__":
    main()
