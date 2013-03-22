#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2013-2013 Ghent University
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
All things moab that are similar to all moab commands from which we want output.

@author Andy Georges
"""


from vsc.utils.fancylogger import getLogger
from vsc.utils.missing import RUDict
from vsc.utils.run import RunAsyncLoop


logger = getLogger('vsc.jobs.moab.checkjob')


class MoabCommand(object):
    """Base class for Moab commands.

    Allows assembling information from a moab command output and store the relevant data somewhere.
    Allows caching of information to handle temporary failure of a target host/cluster.

    This class should be subclassed to allow actual running things
    """

    def __init__(self):
        """Initialise"""

        self.command = None
        self.pickle_name = None
        self.command_option_name = None
        self.parser = None
        self.info = None
        self.opts = None

    def _process_attributes(job_xml, job, attributes):
        """Fill in thee job attributes from the XML data.

        @type job_xml: dom structure for a job
        @type job: dict
        @type attributes: list of strings

        @param job_xml: XML description of a job, as returned by Moab's showq command
        @param job: maops attributes to their values for a job
        @param attributes: list of attributes we'd like to find in the job description

        Only places the attributes than are found in the description in the job disctionary, so no
        extraneous keys are put in the dict.
        """
        for attribute in attributes:
            job[attribute] = job_xml.getAttribute(attribute)
            if not job[attribute]:
                logger.error("Failed to find attribute name %s in %s" % (attribute, job_xml.toxml()))
                job.pop(attribute)

    def _run_moab_command(path, cluster, options, xml=True, process=True):
        """Run the moab command and return the (processed) output.

        @type path: string
        @type options: list of strings
        @type xml: boolean
        @type process: boolean

        @param path: path to the checkjob executable
        @param options: The options to pass to the checkjob command.
        @param xml: Should we ask for output in xml format?
        @param process: Should we do postprocessing of the output here?
                        FIXME: the output format may depend on the options, so this may be fragile.

        @return: string if no processing is done, dict with the job information otherwise
        """
        options_ = options
        if xml:
            options_ += ['--xml']

        (exit_code, output) = RunAsyncLoop.run([path] + options_)

        if exit_code != 0:
            return None

        if process:
            return self.parser(cluster, output)
        else:
            return output

    def get_moab_command_information(moab_command, command_path_option, pickle_file_name_template, info):
        """Accumulate the checkjob information for the users on the given hosts.

        @type opts: vsc.util.generaloption.SimpleOption
        @type moab_command: function that should be called to perform the actual data gathering
        @type command_path_option: the name of the option to retrieve from the configuration
        @type info: class that should be instantiated to gather the data
        """

        job_information = info()
        failed_hosts = []
        reported_hosts = []

        # Obtain the information from all specified hosts
        for host in opts.options.hosts:

            master = opts.configfile_parser.get(host, "master")
            path = opts.configfile_parser.get(host, command_path_option)

            host_job_information = moab_command(path, host, ["--host=%s" % (master)], xml=True, process=True)

            if not host_job_information:
                failed_hosts.append(host)
                logger.error("Couldn't collect info for host %s" % (host))
                logger.info("Trying to load cached pickle file for host %s" % (host))

                host_queue_information = load_pickle_cluster_file(host)
            else:
                store_pickle_cluster_file(host, host_queue_information)

            if not host_queue_information:
                logger.error("Couldn't load info for host %s" % (host))
            else:
                job_information.update(host_queue_information)
                reported_hosts.append(host)

        return (job_information, reported_hosts, failed_hosts)


