#!/usr/bin/env python
"""
`pdaq summarize` script which prints an I3Live-style summary of all
runs found in /mnt/data/pdaq/logs
"""

from __future__ import print_function

import argparse
import logging
import os
import re
import socket
import sys

from ANSIEscapeCode import ANSIEscapeCode
from ClusterDescription import ClusterDescription
from DAQConst import DAQPort
from DAQRPC import RPCClient
from LogSorter import BaseLog
from utils.DashXMLLog import DashXMLLog, DashXMLLogException


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-c", "--use-cnc", dest="use_cnc",
                        action="store_true", default=False,
                        help="Query CnCServer for run details")
    parser.add_argument("-C", "--show-cluster-config", dest="show_clucfg",
                        action="store_true", default=False,
                        help="Include non-standard cluster configurations")
    parser.add_argument("-D", "--log-directory", dest="log_directory",
                        default="/mnt/data/pdaq/log",
                        help=("Directory where 'daqrunXXXXXX' directories"
                              " are stored"))
    parser.add_argument("-n", "--no-color", dest="no_color",
                        action="store_true", default=False,
                        help="Do not add color to output")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Print run details")
    parser.add_argument(dest="files", nargs="*")


class DashLog2RunXML(BaseLog):
    "Recreate run.xml from the information in dash.log"

    DATE_STR = r"(\d+-\d+-\d+ \d+:\d+:\d+\.\d+)"
    DASH_PAT = re.compile(r"^\S+\s+\[" + DATE_STR + r"\]\s+(.*)$")

    LOGVERS_PAT = re.compile(r"Version info: (\S+) (\S+) \S+ \S+Z?")
    OLDVERS_PAT = re.compile(r"Version info: \S+ \S+ \S+ \S+Z? \S+ (\S+)"
                             r" (\S+)")

    RATES_PAT = re.compile(r"(\d+) physics events(\s+\(\d+\.\d+ Hz\))?," +
                           r" (\d+) moni events, (\d+) SN events, (\d+) tcals")

    def __init__(self):
        super(DashLog2RunXML, self).__init__(None)

    @classmethod
    def parse(cls, path, verbose=False):
        has_version = False
        has_config = False
        has_cluster = False
        has_run_num = False

        phys_evts = None
        moni_evts = None
        sn_evts = None
        tcal_evts = None
        end_time = None

        dirname = os.path.dirname(path)
        if not os.path.exists(os.path.join(dirname, "run.xml")):
            xmlname = "run.xml"
        else:
            xmlname = "fake.xml"

        runxml = DashXMLLog(dirname, file_name=xmlname)
        runxml.set_first_good_time(0)
        runxml.set_last_good_time(0)
        runxml.run_status = None

        with open(path, "r") as rdr:
            for line in rdr:
                dmatch = cls.DASH_PAT.match(line)
                if dmatch is None:
                    # ignore irrelevant lines
                    continue

                dash_date = dmatch.group(1)
                dash_text = dmatch.group(2)

                if not has_version:
                    match = cls.LOGVERS_PAT.match(dash_text)
                    if match is not None:
                        runxml.version_info = (match.group(1), match.group(2))
                        has_version = True
                        continue

                    match = cls.OLDVERS_PAT.match(dash_text)
                    if match is not None:
                        runxml.version_info = (match.group(1), match.group(2))
                        has_version = True
                        continue

                if not has_config:
                    target = "Run configuration: "
                    idx = dash_text.find(target)
                    if idx >= 0:
                        runxml.run_config_name = dash_text[idx + len(target):]
                        has_config = True
                        if not has_version:
                            logging.error("Missing \"Version Info\" line"
                                          " from %s", path)
                        continue

                if not has_cluster:
                    target = "Cluster: "
                    idx = dash_text.find(target)
                    if idx >= 0:
                        runxml.cluster_config_name \
                          = dash_text[idx + len(target):]
                        has_cluster = True
                        if not has_config:
                            logging.error("Missing \"Run Configuration\" line"
                                          " from %s", path)
                        continue

                if not has_run_num:
                    target = "Starting run "
                    idx = dash_text.find(target)
                    if idx < 0:
                        target = "Switching to run "
                        idx = dash_text.find(target)
                    if idx >= 0:
                        numstr = dash_text[idx + len(target):]
                        numidx = 0
                        while numstr[numidx - 1] == '.':
                            numidx -= 1
                        if numidx < 0:
                            numstr = numstr[:numidx]

                        runxml.run_number = numstr
                        runxml.start_time = dash_date
                        has_run_num = True
                        if not has_cluster:
                            logging.error("Missing \"Cluster\" line"
                                          " from %s", path)
                        continue

                if dash_text.endswith(" tcals"):
                    match = cls.RATES_PAT.match(dash_text)
                    if match is not None:
                        phys_evts = match.group(1)
                        moni_evts = match.group(3)
                        sn_evts = match.group(4)
                        tcal_evts = match.group(5)
                        end_time = dash_date
                        continue

                if dash_text.find("Run terminated") >= 0:
                    cond = dash_text.find("ERROR") > 0
                    runxml.run_status = cond
                    continue

        if phys_evts is not None:
            runxml.num_physics = phys_evts
            runxml.num_moni = moni_evts
            runxml.num_sn = sn_evts
            runxml.num_tcal = tcal_evts
            runxml.end_time = end_time

        return runxml

    @classmethod
    def from_summary(cls, summary):
        runxml = DashXMLLog()

        runxml.run_number = summary["num"]
        runxml.run_config_name = summary["config"]
        runxml.start_time = summary["startTime"]
        runxml.end_time = summary["endTime"]
        runxml.num_physics = summary["numEvents"]
        runxml.num_moni = summary["numMoni"]
        runxml.num_sn = summary["numSN"]
        runxml.num_tcal = summary["numTcal"]
        if summary["result"] is None:
            termcond = None
        else:
            termcond = summary["result"] == "SUCCESS"
        runxml.run_status = termcond

        return runxml


class Sum(object):
    def __init__(self, logdir):
        self.__logdir = logdir
        self.__dry_run = False
        self.__cnc = None

    @classmethod
    def __compute_duration_and_rate(cls, runxml, verbose=False):
        rate = ""
        duration = "???"

        xml_start_time = runxml.start_time
        xml_end_time = runxml.end_time
        if xml_start_time is not None and xml_end_time is not None:
            timediff = xml_end_time - xml_start_time
            if timediff.days >= 0:
                total = timediff.seconds
                if timediff.days > 0:
                    total += timediff.days * 60 * 60 * 24

                dtotal = total
                dsec = dtotal % 60
                dtotal = int(dtotal / 60)
                dmin = dtotal % 60
                dtotal = int(dtotal / 60)
                dhrs = dtotal % 24
                days = int(dtotal / 24)

                if days == 0:
                    duration = "%02d:%02d:%02d" % (dhrs, dmin, dsec)
                else:
                    duration = "%d:%02d:%02d:%02d" % (days, dhrs, dmin, dsec)

                if verbose:
                    evts = runxml.num_physics
                    if evts is None or total == 0:
                        rate = ""
                    else:
                        rate = "%.02f" % (float(evts) / float(total), )

        return duration, rate

    def __get_run_xml(self, run_num, use_cnc=True):
        cnc = None
        if use_cnc:
            try:
                tmp = self.cnc_connection()
                cnc = tmp
            except ValueError:
                pass

            if cnc is not None:
                try:
                    summary = cnc.rpc_run_summary(run_num)
                    return DashLog2RunXML.from_summary(summary)
                except DashXMLLogException:
                    pass

        try:
            return self.__read_daq_run_dir(run_num)
        except:  # pylint: disable=bare-except
            # couldn't find any details about this run!
            return None

    def __read_daq_run_dir(self, run_num):
        rundir = os.path.join(self.__logdir, "daqrun%05d" % run_num)
        if not os.path.isdir(rundir):
            raise ValueError("Cannot see run %d data" % run_num)

        xmlpath = os.path.join(rundir, "run.xml")
        if os.path.exists(xmlpath):
            runxml = DashXMLLog.parse(rundir)
        else:
            path = os.path.join(rundir, "dash.log")
            if os.path.exists(path):
                runxml = DashLog2RunXML.parse(path)
            else:
                logging.error("Cannot construct fake run.xml file for run %d",
                              run_num)
                return None

        return runxml

    def cnc_connection(self, abort_on_fail=True):
        "Get a connection to CnCServer"
        if self.__cnc is None:
            self.__cnc = RPCClient("localhost", DAQPort.CNCSERVER)
            try:
                self.__cnc.rpc_ping()
            except socket.error as err:
                if err[0] == 61 or err[0] == 111:
                    self.__cnc = None
                else:
                    raise

        if self.__cnc is None and abort_on_fail:
            raise ValueError("Cannot connect to CnCServer")

        return self.__cnc

    @classmethod
    def log_info(cls, msg):
        "Log an INFO message"
        print(msg)

    def report(self, run_num, std_clucfg=None, no_color=False,
               use_cnc=False, verbose=False):
        "Report the details of a run"
        if self.__dry_run:
            return

        runxml = self.__get_run_xml(run_num, use_cnc=use_cnc)
        if runxml is None:
            return

        duration, rate = self.__compute_duration_and_rate(runxml,
                                                          verbose=verbose)

        timestr = str(runxml.start_time)
        idx = timestr.find(".")
        if idx > 0:
            timestr = timestr[:idx]

        cond = runxml.run_status
        if cond is None:
            color = ANSIEscapeCode.FG_WHITE + ANSIEscapeCode.BG_BLUE
            status = "RUNNING"
        elif cond:
            color = ANSIEscapeCode.FG_RED + ANSIEscapeCode.BG_WHITE
            status = "FAILED"
        else:
            color = ANSIEscapeCode.FG_GREEN + ANSIEscapeCode.BG_WHITE
            status = "SUCCESS"
        if not no_color:
            status = color + status + ANSIEscapeCode.OFF

        run = runxml.run_number
        if run is None:
            run = "???"

        if not verbose:
            if std_clucfg is None:
                cfgstr = runxml.run_config_name
            else:
                cfgstr = runxml.cluster_config_name
                if cfgstr is None or cfgstr == "":
                    cfgstr = std_clucfg

            self.log_info("Run %s  %s  %8.8s  %-27.27s : %s" %
                          (run, timestr, duration, cfgstr, status))
            return

        # get cluster config
        cluster = strip_clucfg(runxml.cluster_config_name)

        if std_clucfg is None:
            cfgstr = runxml.run_config_name
            cfgfmt = "%-27.27s"
        else:
            # get config, make sure cluster config is visible
            config = runxml.run_config_name
            if len(config) + len(cluster) + 2 > 35:
                config = config[:35-(len(cluster) + 2)]

            cfgstr = "%s(%s)" % (config, cluster, )
            cfgfmt = "%-35.35s"

        (rel, rev) = runxml.version_info
        if rel is None:
            relstr = ""
        else:
            if rev is None:
                relstr = str(rel)
            else:
                relstr = "%s_%s" % (rel, rev)

        self.log_info(("Run %d  %s  %8.8s  %7s  " + cfgfmt + "  %s : %s") %
                      (run, timestr, duration, rate, cfgstr, relstr, status))


def strip_clucfg(cluster):
    """
    Strip away "-cluster" and/or ".cfg" suffix from cluster config name
    """
    if cluster.endswith(".cfg"):
        cluster = cluster[:-4]
    if cluster.endswith("-cluster"):
        cluster = cluster[:-8]
    return cluster


def summarize(args):
    "Summarize all pDAQ runs found in the log directory"
    if not args.show_clucfg:
        std_clucfg = None
    else:
        clu_name = ClusterDescription.get_cluster_name()
        std_clucfg = strip_clucfg(clu_name)

    if len(args.files) > 0:  # pylint: disable=len-as-condition
        files = args.files[:]
    else:
        files = []
        for entry in os.listdir(args.log_directory):
            if entry.startswith("daqrun"):
                files.append(entry)
        if len(files) == 0:  # pylint: disable=len-as-condition
            raise SystemExit("No 'daqrun' directories found in \"%s\"" %
                             (args.log_directory, ))
    files.sort()

    summary = Sum(args.log_directory)

    for arg in files:
        if arg.find("/") >= 0:
            arg = os.path.basename(arg)

        if arg.startswith("daqrun"):
            arg = arg[6:]

        try:
            num = int(arg)
        except ValueError:
            logging.error("Bad run number \"%s\"", arg)
            continue

        try:
            summary.report(num, std_clucfg=std_clucfg, no_color=args.no_color,
                           verbose=args.verbose, use_cnc=args.use_cnc)
        except:  # pylint: disable=bare-except
            import traceback
            traceback.print_exc()
            logging.error("Bad run %d: %s: %s", num, sys.exc_info()[0],
                          sys.exc_info()[1])


def main():
    "Main program"
    argp = argparse.ArgumentParser()
    add_arguments(argp)
    args = argp.parse_args()

    summarize(args)


if __name__ == "__main__":
    main()
