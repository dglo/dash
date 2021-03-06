#!/usr/bin/env python
"Wrappers for all `pdaq` subcommands"

from __future__ import print_function

import os

from decorators import classproperty
from locate_pdaq import find_pdaq_config, find_pdaq_trunk
from utils.Machineid import Machineid

# find top pDAQ directory
PDAQ_HOME = find_pdaq_trunk()

# list of all pDAQ command objects (classes which add the @command decorator)
COMMANDS = []


def command(cls):
    """
    Decorator which adds a command class to the master list
    """
    COMMANDS.append(cls)
    return cls


class FakeArgParser(object):
    """
    This simulates ArgumentParser.add_arguments() in order to build
    the list of valid arguments for bash
    """
    def __init__(self):
        self.__args = []

    def add_argument(self, *args, **kwargs):  # pylint: disable=unused-argument
        "Simulates argparse.ArgumentParser.add_argument()"
        for arg in args:
            if arg != "" and arg[0] == "-":
                self.__args.append(arg)

    @property
    def arguments(self):
        "Return list of arguments"
        return self.__args


class BaseCmd(object):
    # Command completion for "-C cluster" and a runconfig argument
    CMDTYPE_CARG = "Carg"
    # Command completion for "-C cluster" and "-c runconfig"
    CMDTYPE_CC = "Cc"
    # Command completion for "help <cmd>"
    CMDTYPE_CMD = "Cmd"
    # Command completion for "-C cluster"
    CMDTYPE_CONLY = "Conly"
    # Command completion for directory argument
    CMDTYPE_DONLY = "Donly"
    # Command completion for file argument
    CMDTYPE_FONLY = "Fonly"
    # Command completion for log directory argument
    CMDTYPE_LD = "LD"
    # Command completion for workspace argument
    CMDTYPE_WS = "WS"
    # Command completion for file argument
    CMDTYPE_CHOICE = "Choice"
    # Command doesn't require any completion
    CMDTYPE_NONE = "None"
    # Command completion is unknown
    CMDTYPE_UNKNOWN = "?"

    # list of commands
    COMMANDS = []

    "Basic structure of a 'pdaq' command"
    @classmethod
    def add_arguments(cls, _):
        """
        Argument handling for this subcommand
        """
        return

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_UNKNOWN

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        raise NotImplementedError()

    @classproperty
    def epilog(cls):  # pylint: disable=no-self-argument,no-self-use
        "Optional extra information/instructions for a subcommand"
        return None

    @classmethod
    def is_valid_host(cls, args):
        "Is this command allowed to run on this machine?"
        raise NotImplementedError()

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        "Name of this subcommand"
        raise NotImplementedError()

    @classmethod
    def run(cls, args):
        "Body of this subcommand"
        print("Not running '%s' args <%s>%s" % (cls.name, type(args), args))


class CmdCopyFromHubs(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from copy_from_hubs import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_NONE

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Copy HitSpool files from one or more hub to a local directory"

    @classmethod
    def is_valid_host(cls, args):
        "HitSpool copies are done from hubs"
        host = Machineid().hname
        return not host.startswith("ichub") and not host.startswith("ithub")

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "copy_from_hubs"

    @classmethod
    def run(cls, args):
        from copy_from_hubs import CopyManager
        mgr = CopyManager(args)
        mgr.run()


@command
class CmdCopyHSFiles(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from copy_hs_files import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_NONE

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Copy HitSpool files from a hub to a remote destination"

    @classmethod
    def is_valid_host(cls, args):
        "HitSpool copies are done from hubs"
        host = Machineid().hname
        return host.startswith("ichub") or host.startswith("ithub")

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "copy_hs_files"

    @classmethod
    def run(cls, args):
        from copy_hs_files import copy_files_in_range
        copy_files_in_range(args)


@command
class CmdDeploy(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from DeployPDAQ import add_arguments
        add_arguments(parser, False)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_CARG

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Deploy pDAQ software and associated files to the cluster"

    @classmethod
    def is_valid_host(cls, args):
        "Deployment is done from the build host"
        return Machineid().is_build_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "deploy"

    @classmethod
    def run(cls, args):
        from DeployPDAQ import run_deploy
        run_deploy(args)


@command
class CmdDumpData(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from DumpPayloads import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_FONLY

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Dump a pDAQ data file (hitspool, physics, moni, sn, etc.)"

    @classmethod
    def is_valid_host(cls, args):
        "Any host can dump data"
        return True

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "dumpdata"

    @classmethod
    def run(cls, args):
        from DumpPayloads import dump_payloads
        dump_payloads(args)


@command
class CmdDumpHSDB(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from DumpHitspoolDB import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_FONLY

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Dump the hitspool database list of hit files and contents"

    @classmethod
    def is_valid_host(cls, args):
        "Any host can dump a hitspool DB"
        return True

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "dumphsdb"

    @classmethod
    def run(cls, args):
        from DumpHitspoolDB import dump_db
        dump_db(args)


@command
class CmdFlash(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from RunFlashers import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_CC

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Control a flasher run"

    @classmethod
    def is_valid_host(cls, args):
        "Flashers are run on the control host"
        return Machineid().is_control_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "flash"

    @classmethod
    def run(cls, args):
        from RunFlashers import flash
        flash(args)


@command
class CmdGenerateConfigSet(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from SplitDetector import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_UNKNOWN

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Generate alternate and -no## configs from a full-detector" \
          " run configuration file"

    @classmethod
    def is_valid_host(cls, args):
        "Config files live on the build host"
        return Machineid().is_build_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "generate-config-set"

    @classmethod
    def run(cls, args):
        from SplitDetector import split_detector
        split_detector(args)


@command
class CmdHelp(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument("helpcmd", help="Command name")

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_CMD

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Print the help message for a command"

    @classmethod
    def is_valid_host(cls, args):
        "Any host can get help"
        return True

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "help"

    @classmethod
    def run(cls, args):
        for cmd in COMMANDS:
            if cmd.name == args.helpcmd:
                import argparse
                import sys

                # load an argparse object with this command's arguments
                base = os.path.basename(sys.argv[0])
                parser = argparse.ArgumentParser(prog="%s %s" %
                                                 (base, cmd.name))
                try:
                    cmd.add_arguments(parser)
                except:  # pylint: disable=bare-except
                    pass

                # print command name and description
                print("%s - %s" % (cmd.name, cmd.description))
                print()

                # let argparse deal with the rest of the help message
                parser.print_help()

                return

        print("Unknown command '%s'" % args.helpcmd)


@command
class CmdKill(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from DAQLaunch import add_arguments_both, add_arguments_kill
        add_arguments_both(parser)
        add_arguments_kill(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_NONE

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Kill the pDAQ components running on the cluster"

    @classmethod
    def is_valid_host(cls, args):
        "Only a control host can kill components"
        return Machineid().is_control_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "kill"

    @classmethod
    def run(cls, args):
        from DAQLaunch import ConsoleLogger, check_detector_state, kill

        if not args.force:
            check_detector_state()

        cfg_dir = find_pdaq_config()
        logger = ConsoleLogger()

        args.cluster_desc = None

        kill(cfg_dir, logger, args=args)


@command
class CmdLaunch(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from DAQLaunch import add_arguments_both, add_arguments_launch
        add_arguments_both(parser)
        add_arguments_launch(parser, False)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_CARG

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Start pDAQ components on the cluster"

    @classmethod
    def is_valid_host(cls, args):
        "Only a control host can launch components"
        return Machineid().is_control_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "launch"

    @classmethod
    def run(cls, args):
        from DAQLaunch import ConsoleLogger, check_detector_state, kill, launch

        if not args.force:
            check_detector_state()

        cfg_dir = find_pdaq_config()
        dash_dir = os.path.join(PDAQ_HOME, "dash")

        logger = ConsoleLogger()

        if not args.skipKill:
            kill(cfg_dir, logger, args=args)

        launch(cfg_dir, dash_dir, logger, args=args)


@command
class CmdSwitchEnv(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from SwitchEnv import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_CHOICE

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Select the Python virtual environment for the cluster"

    @classmethod
    def is_valid_host(cls, args):
        "Update is done from the build host"
        mid = Machineid()
        return mid.is_build_host or mid.is_unknown_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "switchenv"

    @classmethod
    def run(cls, args):
        from SwitchEnv import update_virtualenv
        update_virtualenv(args)


@command
class CmdQueueLogs(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from SpadeQueue import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_LD

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Submit pDAQ log files to SPADE for transmission to the North"

    @classmethod
    def is_valid_host(cls, args):
        "Any host can have log files"
        return True

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "queuelogs"

    @classmethod
    def run(cls, args):
        from SpadeQueue import queue_logs
        queue_logs(args)


@command
class CmdRemoveHubs(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from RemoveHubs import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_CARG

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Remove hubs or racks from a run configuration"

    @classmethod
    def is_valid_host(cls, args):
        "Config files live on the build host"
        return Machineid().is_build_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "removehubs"

    @classmethod
    def run(cls, args):
        from RemoveHubs import remove_hubs
        remove_hubs(args)


@command
class CmdRun(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from ExpControlSkel import add_arguments
        add_arguments(parser, False)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_CARG

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Start a pDAQ run (does not communicate with Live)"

    @classmethod
    def is_valid_host(cls, args):
        "Only a control host can start runs"
        return Machineid().is_control_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "run"

    @classmethod
    def run(cls, args):
        from ExpControlSkel import daqrun
        daqrun(args)


@command
class CmdRunNumber(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from RunNumber import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_NONE

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Get/set last run number"

    @classmethod
    def is_valid_host(cls, args):
        "Only a control host can get/set run numbers"
        return Machineid().is_control_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "runnumber"

    @classmethod
    def run(cls, args):
        from RunNumber import get_or_set_run_number
        get_or_set_run_number(args)


@command
class CmdSortLogs(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from LogSorter import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_LD

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Combine all log entries from a run"

    @classmethod
    def is_valid_host(cls, args):
        "Any host can have log files"
        return True

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "sortlogs"

    @classmethod
    def run(cls, args):
        from LogSorter import sort_logs
        sort_logs(args)


@command
class CmdStatus(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from DAQStatus import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_NONE

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Print the status of all active pDAQ components"

    @classmethod
    def is_valid_host(cls, args):
        "Only a control host can check component status"
        return Machineid().is_control_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "status"

    @classmethod
    def run(cls, args):
        from DAQStatus import print_status
        print_status(args)


@command
class CmdStopRun(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from DAQStopRun import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_NONE

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Stop the current pDAQ run (does not communicate with Live)"

    @classmethod
    def is_valid_host(cls, args):
        "Only a control host can emergency-stop runs"
        return Machineid().is_control_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "stoprun"

    @classmethod
    def run(cls, args):
        from DAQStopRun import stoprun
        stoprun(args)


@command
class CmdStdTest(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from StandardTests import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_CONLY

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Run the standard pDAQ test configurations to" \
            " validate the system"

    @classmethod
    def is_valid_host(cls, args):
        """
        Run `pdaq deploy` on build host, run StandardTests on control host
        """
        mid = Machineid()
        return mid.is_build_host or mid.is_control_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "stdtest"

    @classmethod
    def run(cls, args):
        from StandardTests import run_tests
        run_tests(args)


@command
class CmdSummarize(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from Summarize import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_FONLY

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Summarize DAQ runs"

    @classmethod
    def is_valid_host(cls, args):
        "This can be run wherever there are 'daqrunXXXXXX' directories"
        return True

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "summarize"

    @classmethod
    def run(cls, args):
        from Summarize import summarize
        summarize(args)


@command
class CmdTail(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from TailLive import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_NONE

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Add colors to Live's log output"

    @classproperty
    def epilog(cls):  # pylint: disable=no-self-argument
        return "Color choices can be customized in either $HOME/.pdaq_colors" \
            " or in a file pointed to by the PDAQ_COLORS environment" \
            " variable.  Use --print_colors to dump the current choices."

    @classmethod
    def is_valid_host(cls, args):
        "Only makes sense on expcont"
        return Machineid().is_control_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "taillive"

    @classmethod
    def run(cls, args):
        from TailLive import tail_logs
        tail_logs(args)


@command
class CmdTest(CmdStdTest):
    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "test"


@command
class CmdUpdateLeapseconds(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from leapsecond_fetch import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_NONE

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Install the latest NIST leapseconds file"

    @classmethod
    def is_valid_host(cls, args):
        "The leapseconds file should be updated on the build host"
        return Machineid().is_build_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "update_leapseconds"

    @classmethod
    def run(cls, args):
        from leapsecond_fetch import update_leapseconds_file
        update_leapseconds_file(args)


@command
class CmdWorkspace(BaseCmd):
    @classmethod
    def add_arguments(cls, parser):
        from Workspace import add_arguments
        add_arguments(parser)

    @classmethod
    def cmdtype(cls):
        return cls.CMDTYPE_WS

    @classproperty
    def description(cls):  # pylint: disable=no-self-argument
        "One-line description of this subcommand"
        return "Print or change the symlink to the current pDAQ workspace"

    @classmethod
    def is_valid_host(cls, args):
        "Workspaces only exist on build host"
        return Machineid().is_build_host

    @classproperty
    def name(cls):  # pylint: disable=no-self-argument
        return "workspace"

    @classmethod
    def run(cls, args):
        from Workspace import workspace
        workspace(args)


def main():
    "Main program"

    import argparse

    cmdmap = {}
    names = []
    for cmd in COMMANDS:
        cmdmap[cmd.name] = cmd.cmdtype()
        names.append(cmd.name)

    parser = argparse.ArgumentParser()
    parser.add_argument("-a", dest="arglist", choices=names,
                        help="Print list of arguments for a command")
    parser.add_argument("-n", dest="shownames",
                        action="store_true", default=False,
                        help="Print list of all valid command names")
    parser.add_argument("-t", dest="cmdtype", choices=names,
                        help="Print command type")
    args = parser.parse_args()

    if args.cmdtype is not None:
        # print a command's command-completion type
        print(cmdmap[args.cmdtype])
    elif args.arglist is not None:
        # print the command-line arguments for a command
        fakeargs = FakeArgParser()
        for cmd in COMMANDS:
            if cmd.name != args.arglist:
                continue

            try:
                cmd.add_arguments(fakeargs)
            except:  # pylint: disable=bare-except
                pass
        for arg in fakeargs.arguments:
            print(arg)
    else:
        # print the list of commands
        for name in names:
            print(name)


if __name__ == "__main__":
    main()
