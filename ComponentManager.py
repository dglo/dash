#!/usr/bin/env python
#
# DAQ component manager - handle launching and killing a set of components

import os
import signal
import sys

from utils import ip

from CachedConfigName import CachedFile
from DAQConfig import DAQConfigParser
from DAQConfigExceptions import DAQConfigException
from DAQConst import DAQPort
from DAQRPC import RPCClient
from ParallelShell import ParallelShell
from Process import findProcess, processList
from RunCluster import RunComponent
from RunSetState import RunSetState


# Find install location via $PDAQ_HOME, otherwise use locate_pdaq.py
if "PDAQ_HOME" in os.environ:
    metaDir = os.environ["PDAQ_HOME"]
else:
    from locate_pdaq import find_pdaq_trunk
    metaDir = find_pdaq_trunk()

SVN_ID = "$Id: DAQLaunch.py 13550 2012-03-08 23:12:05Z dglo $"


def listComponentRanges(compList):
    """
    Concatenate a list of components into a string showing names and IDs
    """
    compDict = {}
    for c in compList:
        if not c.name() in compDict:
            compDict[c.name()] = [c, ]
        else:
            compDict[c.name()].append(c)

    hasOrder = True

    pairList = []
    for k in sorted(compDict.keys(), key=lambda nm: len(compDict[nm]),
                    reverse=True):
        if len(compDict[k]) == 1 and compDict[k][0].num() == 0:
            if not hasOrder:
                order = compDict[k][0].name()
            else:
                try:
                    order = compDict[k][0].order()
                except AttributeError:
                    hasOrder = False
                    order = compDict[k][0].name()
            pairList.append((compDict[k][0].name(), order))
        else:
            prevNum = None
            rangeStr = k + "#"
            inRange = False
            for c in sorted(compDict[k], key=lambda c: c.num()):
                if prevNum is None:
                    rangeStr += "%d" % c.num()
                elif c.num() == prevNum + 1:
                    if not rangeStr.endswith("-"):
                        rangeStr += "-"
                else:
                    if rangeStr.endswith("-"):
                        rangeStr += "%d" % prevNum
                    rangeStr += ",%d" % c.num()
                prevNum = c.num()

            if rangeStr.endswith("-"):
                rangeStr += "%d" % prevNum

            if not hasOrder:
                order = compDict[k][0].name()
            else:
                try:
                    order = compDict[k][0].order()
                except AttributeError:
                    hasOrder = False
                    order = compDict[k][0].name()
            pairList.append((rangeStr, order))

    strList = []
    for p in sorted(pairList, key=lambda pair: pair[1]):
        strList.append(p[0])

    return ", ".join(strList)


class HostNotFoundForComponent   (Exception):
    pass


class ComponentNotFoundInDatabase(Exception):
    pass


class ComponentManager(object):
    # the pDAQ release name
    #
    RELEASE = "1.0.0-SNAPSHOT"

    # Component Name -> JarParts mapping.  For constructing the name of
    # the proper jar file used for running the component, based on the
    # lower-case name of the component.
    __COMP_JAR_MAP = {
        "eventbuilder": ("eventBuilder-prod", "comp"),
        "secondarybuilders": ("secondaryBuilders", "comp"),
        "inicetrigger": ("trigger", "iitrig"),
        "simpletrigger": ("trigger", "simptrig"),
        "icetoptrigger": ("trigger", "ittrig"),
        "globaltrigger": ("trigger", "gtrig"),
        "amandatrigger": ("trigger", "amtrig"),
        "stringhub": ("StringHub", "comp"),
        "replayhub": ("StringHub", "replay")
        }


    @classmethod
    def __convertDict(cls, compdicts):
        """
        Convert a list of CnCServer component dictionaries
        to a list of component objects
        """
        comps = []
        for c in compdicts:
            lc = RunComponent(c["compName"], c["compNum"], "??logLevel??",
                              "??jvm??", "??jvmArgs??", c["host"], False)
            comps.append(lc)
        return comps

    @classmethod
    def __createAndExpand(cls, dirname, fallbackDir, logger, dryRun=False):
        """
        Create the directory if it doesn't exist.
        Return the fully qualified path
        """
        if dirname is not None:
            if not os.path.isabs(dirname):
                # non-fully-qualified paths are relative
                # to metaproject top dir:
                dirname = os.path.join(metaDir, dirname)
            if not os.path.exists(dirname) and not dryRun:
                try:
                    os.mkdir(dirname)
                except OSError as (errno, strerror):
                    if fallbackDir is None:
                        einfo = sys.exc_info()
                        raise einfo[0], einfo[1], einfo[2]
                    else:
                        if logger is not None:
                            logger.error(("Problem making directory \"%s\"" +
                                          " (%s)") % (dirname, strerror))
                            logger.error("Using fallback directory \"%s\"" %
                                         fallbackDir)
                        dirname = fallbackDir
                        if not os.path.exists(dirname):
                            os.mkdir(dirname)

        return dirname

    @classmethod
    def __getCnCComponents(cls, cncrpc=None, runSetId=None):
        if cncrpc is None:
            cncrpc = RPCClient('localhost', DAQPort.CNCSERVER)

        if runSetId is None:
            unused = cls.__getUnused(cncrpc)
        else:
            unused = []
        runsets = cls.__getRunsets(cncrpc)

        comps = []
        if runSetId is not None:
            if runSetId in runsets:
                comps += cls.__convertDict(runsets[runSetId][1])
        else:
            comps += cls.__convertDict(unused)
            for rs in runsets:
                comps += cls.__convertDict(rs)
        return comps

    @classmethod
    def __getRunsets(cls, cncrpc):
        runsets = []
        ids = cncrpc.rpc_runset_list_ids()
        for runid in ids:
            runsets.append(cncrpc.rpc_runset_list(runid))

        return runsets

    @classmethod
    def __getUnused(cls, cncrpc):
        return cncrpc.rpc_component_list_dicts([], False)

    @classmethod
    def __isRunning(cls, procName, procList):
        "Is this process running?"
        pids = list(findProcess(procName, procList))
        return len(pids) > 0

    @classmethod
    def __reportAction(cls, logger, action, actionList, ignored):
        "Report which daemons were launched/killed and which were ignored"

        if logger is not None:
            if len(actionList) > 0:
                if len(ignored) > 0:
                    logger.info("%s %s, ignored %s" %
                                (action, ", ".join(actionList),
                                 ", ".join(ignored)))
                else:
                    logger.info("%s %s" % (action, ", ".join(actionList)))
            elif len(ignored) > 0:
                logger.info("Ignored %s" % ", ".join(ignored))

    @classmethod
    def buildComponentList(cls, clusterConfig):
        compList = []
        for node in clusterConfig.nodes():
            for comp in node.components():
                if not comp.isControlServer():
                    compList.append(RunComponent(comp.name(), comp.id(),
                                                 comp.logLevel(),
                                                 comp.jvm(),
                                                 comp.jvmArgs(),
                                                 node.hostName(), False))
        return compList

    @classmethod
    def countActiveRunsets(cls):
        "Return the number of active runsets"
        # connect to CnCServer
        cnc = RPCClient('localhost', DAQPort.CNCSERVER)

        # Get the number of active runsets from CnCServer
        try:
            numSets = int(cnc.rpc_runset_count())
        except:
            numSets = 0

        runsets = {}

        active = 0
        if numSets > 0:
            inactiveStates = (RunSetState.READY, RunSetState.IDLE,
                              RunSetState.DESTROYED, RunSetState.ERROR)

            for id in cnc.rpc_runset_list_ids():
                runsets[id] = cnc.rpc_runset_state(id)
                if not runsets[id] in inactiveStates:
                    active += 1

        return (runsets, active)

    @classmethod
    def getActiveComponents(cls, clusterDesc, configDir=None, validate=True,
                            useCnC=False, logger=None):
        if not useCnC:
            comps = None
        else:
            # try to extract component info from CnCServer
            #
            try:
                comps = cls.__getCnCComponents()
                if logger is not None:
                    logger.info("Extracted active components from CnCServer")
            except:
                comps = None

        if comps is None:
            killOnly=False

            try:
                activeConfig = \
                    DAQConfigParser.\
                    getClusterConfiguration(None,
                                            useActiveConfig=True,
                                            clusterDesc=clusterDesc,
                                            configDir=configDir,
                                            validate=validate)
            except DAQConfigException as dce:
                if str(dce).find("RELAXNG") >= 0:
                    exc = sys.exc_info()
                    raise exc[0], exc[1], exc[2]
                activeConfig = None

            if activeConfig is not None:
                comps = cls.buildComponentList(activeConfig)
            else:
                if killOnly:
                    raise SystemExit("DAQ is not currently active")
                comps = []

            if logger is not None:
                if activeConfig is not None:
                    logger.info("Extracted component list from %s" %
                                activeConfig.getConfigName())
                else:
                    logger.info("No active components found")

        return comps

    @classmethod
    def getComponentJar(cls, compName):
        """
        Return the name of the executable jar file for the named component.
        """

        jarParts = cls.__COMP_JAR_MAP.get(compName.lower(), None)
        if not jarParts:
            raise ComponentNotFoundInDatabase(compName)

        return "%s-%s-%s.jar" % (jarParts[0], cls.RELEASE, jarParts[1])

    @classmethod
    def kill(cls, comps, verbose=False, dryRun=False,
             killCnC=True, killWith9=False, logger=None, parallel=None):
        "Kill pDAQ python and java components"

        killed = []
        ignored = []

        serverName = "CnCServer"
        if killCnC:
            if cls.killProcess(serverName, dryRun):
                killed.append(serverName)
        elif not dryRun:
            ignored.append(serverName)

        # clear the active configuration
        if not dryRun:
            CachedFile.clearActiveConfig()

        cls.killComponents(comps, dryRun=dryRun, verbose=verbose,
                           killWith9=killWith9, logger=logger,
                           parallel=parallel)

        if verbose and not dryRun and logger is not None:
            logger.info("DONE killing Java Processes.")
        if len(killed) > 0 or len(ignored) > 0 or len(comps) > 0:
            jstr = listComponentRanges(comps)
            jlist = jstr.split(", ")
            try:
                # CnCServer may be part of the list of launched components
                jlist.remove(serverName)
            except:
                pass
            cls.__reportAction(logger, "Killed", killed + jlist, ignored)

    @classmethod
    def killComponents(cls, compList, dryRun=False, verbose=False,
                       killWith9=False, logger=None, parallel=None):
        if parallel is None:
            parallel = ParallelShell(dryRun=dryRun, verbose=verbose,
                                     trace=verbose, timeout=30)
        cmdToHostDict = {}
        for comp in compList:
            if comp.jvm() is None:
                continue

            if comp.isHub():
                killPat = "stringhub.componentId=%d" % comp.id()
            else:
                killPat = cls.getComponentJar(comp.name())

            if comp.isLocalhost():  # Just kill it
                fmtStr = "pkill %%s -fu %s %s" % (os.environ["USER"], killPat)
            else:
                fmtStr = "ssh %s pkill %%s -f %s" % (comp.host(), killPat)

            # add '-' on first command
            if killWith9:
                add9 = 0
            else:
                add9 = 1

            # only do one pass if we're using 'kill -9'
            for i in range(add9 + 1):
                # set '-9' flag
                if i == add9:
                    niner = "-9"
                else:
                    niner = ""

                # sleep for all commands after the first pass
                if i == 0:
                    sleepr = ""
                else:
                    sleepr = "sleep 2; "

                cmd = sleepr + fmtStr % niner
                if verbose or dryRun:
                    if logger is not None:
                        logger.info(cmd)
                if not dryRun:
                    parallel.add(cmd)
                    cmdToHostDict[cmd] = comp.host()

        if not dryRun:
            parallel.start()
            parallel.wait()

            # check for ssh failures here
            cmd_results_dict = parallel.getCmdResults()
            for cmd in cmd_results_dict:
                rtn_code, results = cmd_results_dict[cmd]
                if cmd in cmdToHostDict:
                    nodeName = cmdToHostDict[cmd]
                else:
                    nodeName = "unknown"
                # pkill return codes
                # 0 -> killed something
                # 1 -> no matched process to kill
                # 1 is okay..  expected if nothing is running
                if rtn_code > 1 and logger is not None:
                    logger.error(("Error non-zero return code ( %s ) "
                                  "for host: %s, cmd: %s") %
                                 (rtn_code, nodeName, cmd))
                    logger.error("Results '%s'" % results)

    @classmethod
    def killProcess(cls, procName, dryRun=False, logger=None):
        pid = int(os.getpid())

        pids = list(findProcess(procName, processList()))

        rtnval = False
        for p in pids:
            if pid != p:
                if dryRun:
                    if logger is not None:
                        logger.info("kill -KILL %d" % p)
                else:
                    os.kill(p, signal.SIGKILL)
                rtnval = True
        return rtnval

    @classmethod
    def launch(cls, doCnC, dryRun, verbose, clusterConfig, dashDir,
               configDir, daqDataDir, logDir, logDirFallback, spadeDir,
               copyDir, logPort, livePort, eventCheck=False,
               checkExists=True, startMissing=True, logger=None, parallel=None,
               forceRestart=True):
        """Launch components"""

        # create missing directories
        spadeDir = cls.__createAndExpand(spadeDir, None, logger, dryRun)
        copyDir = cls.__createAndExpand(copyDir, None, logger, dryRun)
        logDir = cls.__createAndExpand(logDir, logDirFallback, logger, dryRun)
        daqDataDir = cls.__createAndExpand(daqDataDir, None, logger, dryRun)

        # get a list of the running processes
        if not startMissing:
            procList = []
        else:
            procList = processList()

        launched = []
        ignored = []

        progBase = "CnCServer"
        progName = progBase + ".py"

        if startMissing and not doCnC:
            doCnC |= not cls.__isRunning(progName, procList)

        if doCnC:
            path = os.path.join(dashDir, progName)
            options = " -c %s -o %s -q %s" % \
                (configDir, logDir, daqDataDir)
            if spadeDir is not None:
                options += ' -s ' + spadeDir
            if clusterConfig.descName() is not None:
                options += ' -C ' + clusterConfig.descName()
            if logPort is not None:
                options += ' -l localhost:%d' % logPort
            if livePort is not None:
                options += ' -L localhost:%d' % livePort
            if copyDir is not None:
                options += " -a %s" % copyDir
            if not forceRestart:
                options += ' -F'
            if verbose:
                options += ' &'
            else:
                options += ' -d'

            cmd = "%s%s" % (path, options)
            if verbose or dryRun:
                if logger is not None:
                    logger.info(cmd)
            if not dryRun:
                if parallel is None:
                    os.system(cmd)
                else:
                    parallel.system(cmd)
                launched.append(progBase)
        elif not dryRun:
            ignored.append(progBase)

        comps = cls.buildComponentList(clusterConfig)

        cls.startComponents(comps, dryRun, verbose, configDir, daqDataDir,
                            DAQPort.CATCHALL, livePort, eventCheck,
                            checkExists=checkExists, logger=logger,
                            parallel=parallel)

        if verbose and not dryRun and logger is not None:
            logger.info("DONE with starting Java Processes.")
        if len(launched) > 0 or len(ignored) > 0 or len(comps) > 0:
            jstr = listComponentRanges(comps)
            jlist = jstr.split(", ")
            cls.__reportAction(logger, "Launched", launched + jlist, ignored)

        # remember the active configuration
        clusterConfig.writeCacheFile(True)

    @classmethod
    def listComponents(cls):
        return cls.__COMP_JAR_MAP.keys()

    @classmethod
    def startComponents(cls, compList, dryRun, verbose, configDir, daqDataDir,
                        logPort, livePort, eventCheck, checkExists=True,
                        logger=None, parallel=None):
        if parallel is None:
            parallel = ParallelShell(dryRun=dryRun, verbose=verbose,
                                     trace=verbose, timeout=30)

        # The dir where all the "executable" jar files are
        binDir = os.path.join(metaDir, 'target', 'pDAQ-%s-dist' % cls.RELEASE,
                              'bin')
        if checkExists and not os.path.isdir(binDir):
            binDir = os.path.join(metaDir, 'target',
                                  'pDAQ-%s-dist.dir' % cls.RELEASE, 'bin')
            if not os.path.isdir(binDir) and not dryRun:
                raise SystemExit("Cannot find jar file directory \"%s\"" %
                                 binDir)

        # how are I/O streams handled?
        if not verbose:
            quietStr = " </dev/null >/dev/null 2>&1"
        else:
            quietStr = ""

        cmdToHostDict = {}
        for comp in compList:
            if comp.jvm() is None:
                continue

            myIP = ip.getLocalIpAddr(comp.host())
            execJar = os.path.join(binDir, cls.getComponentJar(comp.name()))
            if checkExists and not os.path.exists(execJar) and not dryRun:
                if logger is not None:
                    logger.error("%s jar file does not exist: %s" %
                                 (comp.name(), execJar))
                continue

            javaCmd = comp.jvm()
            jvmArgs = comp.jvmArgs()

            jvmArgs += " -Dicecube.daq.component.configDir='%s'" % configDir
            #switches = "-g %s" % configDir
            switches = "-d %s" % daqDataDir
            switches += " -c %s:%d" % (myIP, DAQPort.CNCSERVER)
            if logPort is not None:
                switches += " -l %s:%d,%s" % (myIP, logPort, comp.logLevel())
            if livePort is not None:
                switches += " -L %s:%d,%s" % (myIP, livePort, comp.logLevel())
            compIO = quietStr

            if comp.isHub():
                jvmArgs += " -Dicecube.daq.stringhub.componentId=%d" % \
                    comp.id()

            if eventCheck and comp.isBuilder():
                jvmArgs += " -Dicecube.daq.eventBuilder.validateEvents"

            baseCmd = "%s %s -jar %s %s %s &" % \
                (javaCmd, jvmArgs, execJar, switches, compIO)
            if comp.isLocalhost():
                # Just run it
                cmd = baseCmd
            else:
                # Have to ssh to run it
                cmd = """ssh -n %s \'sh -c \"%s\"%s &\'""" % \
                    (comp.host(), baseCmd, quietStr)
            cmdToHostDict[cmd] = comp.host()
            if verbose or dryRun:
                if logger is not None:
                    logger.info(cmd)
            if not dryRun:
                parallel.add(cmd)

        if verbose and not dryRun:
            parallel.showAll()
        if not dryRun:
            parallel.start()
            if not verbose:
                # if we wait during verbose mode, the program hangs
                parallel.wait()

                # check for ssh failures here
                cmd_results_dict = parallel.getCmdResults()
                for cmd in cmd_results_dict:
                    rtn_code, results = cmd_results_dict[cmd]
                    if cmd in cmdToHostDict:
                        nodeName = cmdToHostDict[cmd]
                    else:
                        nodeName = "unknown"
                    if rtn_code != 0 and logger is not None:
                        logger.error(("Error non zero return code ( %s )" +
                                      " for host: %s, cmd: %s") %
                                     (rtn_code, nodeName, cmd))
                        logger.error("Results '%s'" % results)

if __name__ == '__main__': pass