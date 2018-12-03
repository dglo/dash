#!/usr/bin/env python

from __future__ import print_function

import os
import socket
import sys
import traceback

from xml.dom import minidom, Node

from Component import Component
from locate_pdaq import find_pdaq_config
from xmlparser import XMLBadFileError, XMLFormatError, XMLParser
from utils.Machineid import Machineid


class ClusterDescriptionFormatError(XMLFormatError):
    pass


class ClusterDescriptionException(Exception):
    pass


class ConfigXMLBase(XMLParser):
    def __init__(self, configDir, configName, suffix='.xml'):
        self.name = None
        fileName = self.buildPath(configDir, configName, suffix=suffix)
        if not os.path.exists(configDir):
            raise XMLBadFileError("Config directory \"%s\" does not exist" %
                                  configDir)
        if fileName is None:
            raise XMLBadFileError('Cannot find "%s" in "%s"' %
                                  (configName, configDir))
        if configName.endswith(suffix):
            configName = configName[:-len(suffix)]

        self.__load_xml(fileName)

        self.__path = fileName
        self.__mtime = os.stat(self.__path).st_mtime
        self.__configName = configName

    def __load_xml(self, path):
        try:
            dom = minidom.parse(path)
        except Exception as exc:
            raise XMLFormatError('%s: %s' % (path, str(exc)))

        self.extractFrom(dom)

    @property
    def configName(self):
        return self.__configName

    def extractFrom(self, dom):
        raise NotImplementedError('extractFrom method is not implemented')

    def loadIfChanged(self, newPath=None):
        if newPath is not None and newPath != self.__path:
            self.__path = newPath
            self.__mtime = 0

        new_mtime = os.stat(self.__path).st_mtime
        if new_mtime == self.__mtime:
            return False

        self.__load_xml(self.__path)

        self.__mtime = new_mtime

        return True


class ClusterComponent(Component):
    def __init__(self, name, num, logLevel=None, required=False):
        self.__required = required

        super(ClusterComponent, self).__init__(name, num, logLevel=logLevel)

    def __str__(self):
        if self.__required:
            rStr = " REQUIRED"
        else:
            rStr = ""

        iStr = self.internalStr
        if iStr is None:
            iStr = ""
        elif len(iStr) > 0:
            iStr = "(%s)" % iStr

        return "%s@%s%s%s" % \
            (self.fullname, str(self.logLevel), iStr, rStr)

    @property
    def hasHitSpoolOptions(self):
        return False

    @property
    def hasJVMOptions(self):
        return False

    @property
    def internalStr(self):
        return None

    @property
    def isControlServer(self):
        return self.name == ControlComponent.NAME

    @property
    def isSimHub(self):
        return False

    @property
    def required(self):
        return self.__required


class ControlComponent(ClusterComponent):
    NAME = "CnCServer"

    def __init__(self):
        super(ControlComponent, self).__init__(ControlComponent.NAME, 0, None,
                                               True)

    def __str__(self):
        return self.name


class JVMArgs(object):
    def __init__(self, path, isServer, heapInit, heapMax, args, extraArgs):
        self.__path = path
        self.__is_server = isServer
        self.__heap_init = heapInit
        self.__heap_max = heapMax
        self.__args = args
        self.__extra_args = extraArgs

    def __str__(self):
        outstr = None
        if self.__path is None:
            outstr = "?"
        else:
            outstr = self.__path

        if self.__is_server is not None and not self.__is_server:
            outstr += " !server"

        if self.__heap_init is not None:
            outstr += " ms=" + self.__heap_init
        if self.__heap_max is not None:
            outstr += " mx=" + self.__heap_max

        if self.__args is not None:
            outstr += " | " + self.__args

        if self.__extra_args is not None:
            outstr += " | " + self.__extra_args

        return outstr

    @property
    def args(self):
        return self.__args

    @property
    def extraArgs(self):
        return self.__extra_args

    @property
    def heapInit(self):
        return self.__heap_init

    @property
    def heapMax(self):
        return self.__heap_max

    @property
    def isServer(self):
        return self.__is_server is True

    @property
    def path(self):
        return self.__path


class JavaComponent(ClusterComponent):
    def __init__(self, name, num, logLevel=None, required=False):
        super(JavaComponent, self).__init__(name, num, logLevel=logLevel,
                                            required=required)

        self.__jvm = None

    @property
    def hasJVMOptions(self):
        return self.__jvm is not None

    @property
    def internalStr(self):
        superStr = super(JavaComponent, self).internalStr
        if self.__jvm is None:
            return superStr

        jvmStr = str(self.__jvm)
        if superStr is None or len(superStr) == 0:
            return jvmStr
        elif jvmStr is None or len(jvmStr) == 0:
            return superStr
        return superStr + " | " + jvmStr

    @property
    def jvmArgs(self):
        if self.__jvm is None:
            raise ClusterDescriptionException("JVM options have not been set")
        return self.__jvm.args

    @property
    def jvmExtraArgs(self):
        if self.__jvm is None:
            raise ClusterDescriptionException("JVM options have not been set")
        return self.__jvm.extraArgs

    @property
    def jvmHeapInit(self):
        if self.__jvm is None:
            raise ClusterDescriptionException("JVM options have not been set")
        return self.__jvm.heapInit

    @property
    def jvmHeapMax(self):
        if self.__jvm is None:
            raise ClusterDescriptionException("JVM options have not been set")
        return self.__jvm.heapMax

    @property
    def jvmPath(self):
        if self.__jvm is None:
            raise ClusterDescriptionException("JVM options have not been"
                                              " set for %s" % self.fullname)
        return self.__jvm.path

    @property
    def jvmServer(self):
        if self.__jvm is None:
            raise ClusterDescriptionException("JVM options have not been"
                                              " set for %s" % self.fullname)
        return self.__jvm.isServer

    @property
    def jvmStr(self):
        if self.__jvm is None:
            return "jvm[???]"
        return str(self.__jvm)

    @property
    def numReplayFilesToSkip(self):
        """Return the number of replay files to skip (None if not specified)"""
        return None

    def setJVMOptions(self, defaults, path, isServer, heapInit, heapMax, args,
                      extraArgs):
        # fill in default values for all unspecified JVM quantities
        if path is None:
            path = None if defaults is None \
                   else defaults.find(self.name, 'jvmPath')
            if path is None and defaults is not None and \
               defaults.JVM is not None:
                path = defaults.JVM.path
        if isServer is None:
            isServer = None if defaults is None \
                       else defaults.find(self.name, 'jvmServer')
            if isServer is None and defaults is not None and \
               defaults.JVM is not None:
                isServer = defaults.JVM.isServer
            if isServer is None:
                isServer = False
        if heapInit is None:
            heapInit = None if defaults is None \
                       else defaults.find(self.name, 'jvmHeapInit')
            if heapInit is None and defaults is not None and \
               defaults.JVM is not None:
                heapInit = defaults.JVM.heapInit
        if heapMax is None:
            heapMax = None if defaults is None \
                      else defaults.find(self.name, 'jvmHeapMax')
            if heapMax is None and defaults is not None and \
               defaults.JVM is not None:
                heapMax = defaults.JVM.heapMax
        if args is None:
            args = None if defaults is None \
                   else defaults.find(self.name, 'jvmArgs')
            if args is None and defaults is not None and \
               defaults.JVM is not None:
                args = defaults.JVM.args
        if extraArgs is None:
            extraArgs = None if defaults is None \
                        else defaults.find(self.name, 'jvmExtraArgs')
            if extraArgs is None and defaults is not None and \
               defaults.JVM is not None:
                extraArgs = defaults.JVM.extraArgs

        self.__jvm = JVMArgs(path, isServer, heapInit, heapMax, args,
                             extraArgs)


class HSArgs(object):
    def __init__(self, directory, interval, max_files):
        self.__directory = directory
        self.__interval = interval
        self.__max_files = max_files

    def __str__(self):
        outstr = None
        if self.__directory is None:
            outstr = "?"
        else:
            outstr = self.__directory

        if self.__interval is not None:
            outstr += " ival=%s" % self.__interval
        if self.__max_files is not None:
            outstr += " max=%s" % self.__max_files

        return outstr

    @property
    def directory(self):
        return self.__directory

    @property
    def interval(self):
        return self.__interval

    @property
    def maxFiles(self):
        return self.__max_files


class HubComponent(JavaComponent):
    def __init__(self, name, num, logLevel=None, required=False):
        super(HubComponent, self).__init__(name, num, logLevel=logLevel,
                                           required=required)

        self.__hs = None
        self.__ntpHost = None
        self.__alertEMail = None

    @property
    def alertEMail(self):
        return self.__alertEMail

    @property
    def hasHitSpoolOptions(self):
        return True

    @property
    def hasReplayOptions(self):
        return False

    @property
    def hitspoolDirectory(self):
        if self.__hs is None:
            raise ClusterDescriptionException("HitSpool options have not" +
                                              " been set")
        return self.__hs.directory

    @property
    def hitspoolInterval(self):
        if self.__hs is None:
            raise ClusterDescriptionException("HitSpool options have not" +
                                              " been set")
        return self.__hs.interval

    @property
    def hitspoolMaxFiles(self):
        if self.__hs is None:
            raise ClusterDescriptionException("HitSpool options have not" +
                                              " been set")
        return self.__hs.maxFiles

    @property
    def internalStr(self):
        if self.__hs is None:
            istr = "hs[???]"
        else:
            istr = "hs[%s]" % str(self.__hs)
        if self.__alertEMail is not None:
            istr += " | alert=%s" % self.__alertEMail
        if self.__ntpHost is not None:
            istr += " | ntp=%s" % self.__ntpHost
        return istr

    @property
    def isRealHub(self):
        return True

    @property
    def ntpHost(self):
        return self.__ntpHost

    def setHitSpoolOptions(self, defaults, directory, interval, maxFiles):
        if directory is None and defaults is not None:
            directory = defaults.find(self.name, 'hitspoolDirectory')
        if interval is None and defaults is not None:
            interval = defaults.find(self.name, 'hitspoolInterval')
        if maxFiles is None and defaults is not None:
            maxFiles = defaults.find(self.name, 'hitspoolMaxFiles')
        self.__hs = HSArgs(directory, interval, maxFiles)

    def setHubOptions(self, defaults, alertEMail, ntpHost):
        if ntpHost is None and defaults is not None:
            ntpHost = defaults.find(self.name, 'ntpHost')
        if alertEMail is None and defaults is not None:
            alertEMail = defaults.find(self.name, 'alertEMail')

        self.__ntpHost = ntpHost
        self.__alertEMail = alertEMail


class ReplayHubComponent(HubComponent):
    def __init__(self, name, num, logLevel=None, required=False):
        super(ReplayHubComponent, self).__init__(name, num, logLevel=logLevel,
                                                 required=required)

        self.__numToSkip = None

    @property
    def hasReplayOptions(self):
        return True

    @property
    def internalStr(self):
        istr = super(ReplayHubComponent, self).internalStr
        if self.__numToSkip is not None:
            istr += " skip=%s" % (self.__numToSkip, )
        return istr

    @property
    def isRealHub(self):
        return False

    @property
    def numReplayFilesToSkip(self):
        """Return the number of replay files to skip (None if not specified)"""
        return self.__numToSkip

    def setNumberToSkip(self, value):
        self.__numToSkip = value


class SimHubComponent(JavaComponent):
    def __init__(self, host, number, priority, ifUnused):
        self.__host = host
        self.__number = number
        self.__priority = priority
        self.__ifUnused = ifUnused

        super(SimHubComponent, self).__init__("SimHub", 0, logLevel=None,
                                              required=False)

    def __str__(self):
        if self.__ifUnused:
            uStr = "(ifUnused)"
        else:
            uStr = ""
        return "%s*%d^%d%s" % \
            (self.__host, self.__number, self.__priority, uStr)

    @property
    def host(self):
        return self.__host

    @property
    def ifUnused(self):
        return self.__ifUnused

    @property
    def isSimHub(self):
        return True

    @property
    def number(self):
        return self.__number

    @property
    def priority(self):
        return self.__priority


class ClusterHost(object):
    def __init__(self, name):
        self.name = name
        self.compMap = {}
        self.simHubs = None
        self.ctlServer = False

    def __cmp__(self, other):
        return cmp(self.name, str(other))

    def __str__(self):
        return self.name

    def addComponent(self, name, num, logLevel, required=False):
        if name.endswith("Hub"):
            comp = HubComponent(name, num, logLevel, required)
        elif name == ControlComponent.NAME:
            comp = ControlComponent(name, num, logLevel, required)
        else:
            comp = JavaComponent(name, num, logLevel, required)

        return self.addComponentObject(comp)

    def addComponentObject(self, comp):
        compKey = comp.fullname
        if compKey in self.compMap:
            errMsg = 'Multiple entries for component "%s" in host "%s"' % \
                (compKey, self.name)
            raise ClusterDescriptionFormatError(errMsg)
        self.compMap[compKey] = comp

        return comp

    def addSimulatedHub(self, num, prio, ifUnused):
        newHub = SimHubComponent(self, num, prio, ifUnused)

        if self.simHubs is None:
            self.simHubs = []
        for sh in self.simHubs:
            if prio == sh.priority:
                errMsg = 'Multiple <simulatedHub> nodes at prio %d for %s' % \
                         (prio, self.name)
                raise ClusterDescriptionFormatError(errMsg)
        self.simHubs.append(newHub)
        return newHub

    def dump(self, fd=None, prefix=None):
        if fd is None:
            fd = sys.stdout
        if prefix is None:
            prefix = ""

        print("%sHost %s:" % (prefix, self.name), file=fd)

        cKeys = sorted(self.compMap.keys())

        for key in cKeys:
            comp = self.compMap[key]
            print("%s  Comp %s" % (prefix, str(comp)), file=fd)

        if self.simHubs is not None:
            for sh in self.simHubs:
                if sh.ifUnused:
                    uStr = " (ifUnused)"
                else:
                    uStr = ""
                print("%s  SimHub*%d prio %d%s" % \
                    (prefix, sh.number, sh.priority, uStr), file=fd)

        if self.ctlServer:
            print("%s  ControlServer" % prefix, file=fd)

    def getComponents(self):
        return list(self.compMap.values())

    @property
    def isControlServer(self):
        return self.ctlServer

    def merge(self, host):
        if self.name != host.name:
            raise AttributeError("Cannot merge host \"%s\" entry into \"%s\"" %
                                 (host.name, self.name))

        if host.ctlServer:
            self.ctlServer = True

        for comp in host.compMap.values():
            key = comp.fullname
            if key in self.compMap:
                errMsg = 'Multiple entries for component "%s" in host "%s"' % \
                         (key, host.name)
                raise ClusterDescriptionFormatError(errMsg)
            self.compMap[key] = comp

        if host.simHubs is not None:
            if self.simHubs is None:
                self.simHubs = []
            for scomp in host.simHubs:
                self.simHubs.append(scomp)

    def setControlServer(self):
        self.ctlServer = True


class ClusterDefaults(object):
    def __init__(self):
        self.Components = {}
        self.HS = HSArgs(None, None, None)
        self.JVM = JVMArgs(None, None, None, None, None, None)
        self.LogLevel = ClusterDescription.DEFAULT_LOG_LEVEL

    def __str__(self):
        if not self.Components:
            cstr = ""
        else:
            cstr = ", " + str(self.Components)

        return "ClusterDefaults[hs %s, jvm %s, logLvl %s, args %s]" % \
            (self.HS, self.JVM, self.LogLevel, cstr)

    def find(self, compName, valName):
        if compName is not None and \
                self.Components is not None and \
                compName in self.Components and \
                valName in self.Components[compName]:
            return self.Components[compName][valName]

        if valName == 'hitspoolDirectory':
            return self.HS.directory
        elif valName == 'hitspoolInterval':
            return self.HS.interval
        elif valName == 'hitspoolMaxFiles':
            return self.HS.maxFiles

        if valName == 'jvmPath':
            return self.JVM.path
        elif valName == 'jvmServer':
            return self.JVM.isServer
        elif valName == 'jvmHeapInit':
            return self.JVM.heapInit
        elif valName == 'jvmHeapMax':
            return self.JVM.heapMax
        elif valName == 'jvmArgs':
            return self.JVM.args
        elif valName == 'jvmExtraArgs':
            return self.JVM.extraArgs

        if valName == 'logLevel':
            return self.LogLevel

        return None


class ClusterDescription(ConfigXMLBase):
    LOCAL = "localhost"
    SPS = "sps"
    SPTS = "spts"
    MDFL = "mdfl"

    DBTYPE_TEST = "test"
    DBTYPE_PROD = "production"
    DBTYPE_NONE = "none"

    DEFAULT_DATA_DIR = "/mnt/data/pdaqlocal"
    DEFAULT_LOG_DIR = "/mnt/data/pdaq/log"
    DEFAULT_LOG_LEVEL = "WARN"

    DEFAULT_PKGSTAGE_DIR = "/software/stage/pdaq/dependencies/tar"
    DEFAULT_PKGINSTALL_DIR = "/software/pdaq"

    def __init__(self, configDir=None, configName=None, suffix='.cfg'):
        self.name = None
        self.__host_map = None
        self.__defaults = None

        self.__spade_log_dir = None
        self.__log_dir_copies = None
        self.__daq_data_dir = None
        self.__daq_log_dir = None
        self.__pkg_stage_dir = None
        self.__pkg_install_dir = None

        self.__default_hs = HSArgs(None, None, None)
        self.__default_jvm = JVMArgs(None, None, None, None, None, None)
        self.__default_log_level = self.DEFAULT_LOG_LEVEL

        if configName is None:
            configName = self.getClusterName()

        if configDir is None:
            configDir = find_pdaq_config()

        try:
            super(ClusterDescription, self).__init__(configDir, configName,
                                                     suffix)
        except XMLBadFileError:
            saved_ex = sys.exc_info()

            if not configName.endswith('.cfg'):
                retryName = configName
            else:
                retryName = configName[:-4]

            if not retryName.endswith('-cluster'):
                retryName += '-cluster'

            try:
                super(ClusterDescription, self).__init__(configDir, retryName,
                                                         suffix)
                configName = retryName
            except XMLBadFileError:
                raise saved_ex[0], saved_ex[1], saved_ex[2]

        derivedName, ext = os.path.splitext(os.path.basename(configName))
        if derivedName.endswith("-cluster"):
            derivedName = derivedName[:-8]
        if derivedName != self.name:
            self.name = derivedName

    def __str__(self):
        return self.name

    @classmethod
    def ___parse_component_node(cls, clusterName, defaults, host, node):
        "Parse a <component> node from a cluster configuration file"
        name = cls.getValue(node, 'name')
        if name is None:
            errMsg = ('Cluster "%s" host "%s" has <component> node' +
                      ' without "name" attribute') % (clusterName, host.name)
            raise ClusterDescriptionFormatError(errMsg)

        idStr = cls.getValue(node, 'id', '0')
        try:
            num = int(idStr)
        except ValueError:
            errMsg = ('Cluster "%s" host "%s" component '
                      '"%s" has bad ID "%s"') % \
                (clusterName, host.name, name, idStr)
            raise ClusterDescriptionFormatError(errMsg)

        # look for optional logLevel
        logLvl = cls.getValue(node, 'logLevel')
        if logLvl is None:
            logLvl = defaults.find(name, 'logLevel')
            if logLvl is None:
                logLvl = defaults.LogLevel

        # look for "required" attribute
        reqStr = cls.getValue(node, 'required')
        required = cls.parseBooleanString(reqStr) is True

        comp = host.addComponent(name, num, logLvl, required=required)

        (jvmPath, jvmServer, jvmHeapInit, jvmHeapMax, jvmArgs,
         jvmExtraArgs) = cls.__parse_jvm_nodes(node)
        comp.setJVMOptions(defaults, jvmPath, jvmServer, jvmHeapInit,
                           jvmHeapMax, jvmArgs, jvmExtraArgs)

        if comp.isRealHub:
            alertEMail = cls.getValue(node, 'alertEMail')
            ntpHost = cls.getValue(node, 'ntpHost')

            comp.setHubOptions(defaults, alertEMail, ntpHost)

            (hsDir, hsInterval, hsMaxFiles) = cls.__parse_hs_nodes(name, node)
            comp.setHitSpoolOptions(defaults, hsDir, hsInterval, hsMaxFiles)

    def __parse_default_nodes(self, cluName, defaults, node):
        """load JVM defaults"""
        (hsDir, hsIval, hsMaxF) = \
            self.__parse_hs_nodes(cluName, node)
        defaults.HS = HSArgs(hsDir, hsIval, hsMaxF)

        (path, isServer, heapInit, heapMax, args, extraArgs) = \
            self.__parse_jvm_nodes(node)
        defaults.JVM = JVMArgs(path, isServer, heapInit, heapMax, args,
                               extraArgs)

        for kid in node.childNodes:
            if kid.nodeType != Node.ELEMENT_NODE:
                continue

            if kid.nodeName == 'logLevel':
                defaults.LogLevel = self.getChildText(kid)
            elif kid.nodeName == 'component':
                name = self.getValue(kid, 'name')
                if name is None:
                    errMsg = ('Cluster "%s" default section has <component>' +
                              ' node without "name" attribute') % cluName
                    raise ClusterDescriptionFormatError(errMsg)

                if name not in defaults.Components:
                    defaults.Components[name] = {}

                (hsDir, hsIval, hsMaxF) = self.__parse_hs_nodes(name, kid)
                if hsDir is not None:
                    defaults.Components[name]['hitspoolDirectory'] = hsDir
                if hsIval is not None:
                    defaults.Components[name]['hitspoolInterval'] = hsIval
                if hsMaxF is not None:
                    defaults.Components[name]['hitspoolMaxFiles'] = hsMaxF

                (path, isServer, heapInit, heapMax, args, extraArgs) = \
                    self.__parse_jvm_nodes(kid)
                if path is not None:
                    defaults.Components[name]['jvmPath'] = path
                if isServer is not None:
                    defaults.Components[name]['jvmServer'] = isServer
                if heapInit is not None:
                    defaults.Components[name]['jvmHeapInit'] = heapInit
                if heapMax is not None:
                    defaults.Components[name]['jvmHeapMax'] = heapMax
                if args is not None:
                    defaults.Components[name]['jvmArgs'] = args
                if extraArgs is not None:
                    defaults.Components[name]['jvmExtraArgs'] = extraArgs

                for cKid in kid.childNodes:
                    if cKid.nodeType == Node.ELEMENT_NODE and \
                       cKid.nodeName == 'logLevel':
                        defaults.Components[name]['logLevel'] = \
                            self.getChildText(cKid)
                        continue

                    if cKid.nodeType == Node.ELEMENT_NODE and \
                       cKid.nodeName == 'alertEMail':
                        defaults.Components[name]['alertEMail'] = \
                            self.getChildText(cKid)
                        continue

                    if cKid.nodeType == Node.ELEMENT_NODE and \
                       cKid.nodeName == 'ntpHost':
                        defaults.Components[name]['ntpHost'] = \
                            self.getChildText(cKid)
                        continue

    @classmethod
    def __parse_host_nodes(cls, name, defaults, hostNodes):
        hostMap = {}
        compToHost = {}

        for node in hostNodes:
            hostname = cls.getValue(node, 'name')
            if hostname is None:
                errMsg = ('Cluster "%s" has <host> node without "name"' +
                          ' attribute') % name
                raise ClusterDescriptionFormatError(errMsg)

            host = ClusterHost(hostname)

            for kid in node.childNodes:
                if kid.nodeType != Node.ELEMENT_NODE:
                    continue

                if kid.nodeName == 'component':
                    cls.___parse_component_node(name, defaults, host, kid)
                elif kid.nodeName == 'controlServer':
                    host.setControlServer()
                elif kid.nodeName == 'simulatedHub':
                    cls.__parse_simhub_node(name, defaults, host, kid)

            # add host to internal host dictionary
            if hostname not in hostMap:
                hostMap[hostname] = host
            else:
                hostMap[hostname].merge(host)

            for comp in host.getComponents():
                compKey = comp.fullname
                if compKey in compToHost:
                    errMsg = 'Multiple entries for component "%s"' % compKey
                    raise ClusterDescriptionFormatError(errMsg)
                compToHost[compKey] = host

        return hostMap

    @classmethod
    def __parse_hs_nodes(cls, name, node):
        # create all hitspool-related variables
        hsDir = None
        interval = None
        maxFiles = None

        # look for jvm node
        for hsNode in cls.getChildNodes(node, 'hitspool'):
            tmpDir = cls.getAttr(hsNode, 'directory')
            if tmpDir is not None:
                hsDir = os.path.expanduser(tmpDir)
            tmpStr = cls.getAttr(hsNode, 'interval', defaultVal=interval)
            if tmpStr is not None:
                interval = float(tmpStr)
            tmpStr = cls.getAttr(hsNode, 'maxfiles', defaultVal=maxFiles)
            if tmpStr is not None:
                maxFiles = int(tmpStr)

        return (hsDir, interval, maxFiles)

    @classmethod
    def __parse_jvm_nodes(cls, node):
        # create all JVM-related variables
        path = None
        isServer = None
        heapInit = None
        heapMax = None
        args = None
        extraArgs = None

        # look for jvm node
        for jvmNode in cls.getChildNodes(node, 'jvm'):
            tmpPath = cls.getAttr(jvmNode, 'path')
            if tmpPath is not None:
                path = os.path.expanduser(tmpPath)
            tmpSrvr = cls.getAttr(jvmNode, 'server')
            if tmpSrvr is not None:
                isServer = cls.parseBooleanString(tmpSrvr)
            heapInit = cls.getAttr(jvmNode, 'heapInit', defaultVal=heapInit)
            heapMax = cls.getAttr(jvmNode, 'heapMax', defaultVal=heapMax)
            args = cls.getAttr(jvmNode, 'args')
            extraArgs = cls.getAttr(jvmNode, 'extraArgs', defaultVal=extraArgs)

        return (path, isServer, heapInit, heapMax, args, extraArgs)

    @classmethod
    def __parse_simhub_node(cls, clusterName, defaults, host, node):
        "Parse a <simulatedHub> node from a cluster configuration file"
        numStr = cls.getValue(node, 'number', '0')
        try:
            num = int(numStr)
        except ValueError:
            errMsg = ('Cluster "%s" host "%s" has <simulatedHub> node with' +
                      ' bad number "%s"') % (clusterName, host.name, numStr)
            raise ClusterDescriptionFormatError(errMsg)

        prioStr = cls.getValue(node, 'priority')
        if prioStr is None:
            errMsg = ('Cluster "%s" host "%s" has <simulatedHub> node' +
                      ' without "priority" attribute') % \
                      (clusterName, host.name)
            raise ClusterDescriptionFormatError(errMsg)
        try:
            prio = int(prioStr)
        except ValueError:
            errMsg = ('Cluster "%s" host "%s" has <simulatedHub> node' +
                      ' with bad priority "%s"') % \
                      (clusterName, host.name, prioStr)
            raise ClusterDescriptionFormatError(errMsg)

        ifStr = cls.getValue(node, 'ifUnused')
        ifUnused = cls.parseBooleanString(ifStr) is True

        comp = host.addSimulatedHub(num, prio, ifUnused)

        (jvmPath, jvmServer, jvmHeapInit, jvmHeapMax, jvmArgs,
         jvmExtraArgs) = cls.__parse_jvm_nodes(node)
        comp.setJVMOptions(defaults, jvmPath, jvmServer, jvmHeapInit,
                           jvmHeapMax, jvmArgs, jvmExtraArgs)

        return host

    @property
    def daqDataDir(self):
        if self.__daq_data_dir is None:
            return self.DEFAULT_DATA_DIR
        return self.__daq_data_dir

    @property
    def daqLogDir(self):
        if self.__daq_log_dir is None:
            return self.DEFAULT_LOG_DIR
        return self.__daq_log_dir

    def defaultAlertEMail(self, compName=None):
        return self.__defaults.find(compName, 'alertEMail')

    def defaultHSDirectory(self, compName=None):
        return self.__defaults.find(compName, 'hitspoolDirectory')

    def defaultHSInterval(self, compName=None):
        return self.__defaults.find(compName, 'hitspoolInterval')

    def defaultHSMaxFiles(self, compName=None):
        return self.__defaults.find(compName, 'hitspoolMaxFiles')

    def defaultJVMArgs(self, compName=None):
        return self.__defaults.find(compName, 'jvmArgs')

    def defaultJVMExtraArgs(self, compName=None):
        return self.__defaults.find(compName, 'jvmExtraArgs')

    def defaultJVMHeapInit(self, compName=None):
        return self.__defaults.find(compName, 'jvmHeapInit')

    def defaultJVMHeapMax(self, compName=None):
        return self.__defaults.find(compName, 'jvmHeapMax')

    def defaultJVMPath(self, compName=None):
        return self.__defaults.find(compName, 'jvmPath')

    def defaultJVMServer(self, compName=None):
        return self.__defaults.find(compName, 'jvmServer')

    def defaultLogLevel(self, compName=None):
        return self.__defaults.find(compName, 'logLevel')

    def defaultNTPHost(self, compName=None):
        return self.__defaults.find(compName, 'ntpHost')

    def dump(self, fd=None, prefix=None):
        if fd is None:
            fd = sys.stdout
        if prefix is None:
            prefix = ""

        print("%sDescription %s" % (prefix, self.name), file=fd)
        if self.__spade_log_dir is not None:
            print("%s  SPADE log directory: %s" % \
                (prefix, self.__spade_log_dir), file=fd)
        if self.__log_dir_copies is not None:
            print("%s  Copied log directory: %s" % \
                (prefix, self.__log_dir_copies), file=fd)
        if self.__daq_data_dir is not None:
            print("%s  DAQ data directory: %s" % \
                (prefix, self.__daq_data_dir), file=fd)
        if self.__daq_log_dir is not None:
            print("%s  DAQ log directory: %s" % \
                (prefix, self.__daq_log_dir), file=fd)
        if self.__pkg_stage_dir is not None:
            print("%s  Package staging directory: %s" % \
                (prefix, self.__pkg_stage_dir), file=fd)
        if self.__pkg_install_dir is not None:
            print("%s  Package installation directory: %s" % \
                (prefix, self.__pkg_install_dir), file=fd)

        if self.__default_hs is not None:
            if self.__default_hs.directory is not None:
                print("%s  Default HS directory: %s" % \
                    (prefix, self.__default_hs.directory), file=fd)
            if self.__default_hs.interval is not None:
                print("%s  Default HS interval: %s" % \
                    (prefix, self.__default_hs.interval), file=fd)
            if self.__default_hs.maxFiles is not None:
                print("%s  Default HS max files: %s" % \
                    (prefix, self.__default_hs.maxFiles), file=fd)

        if self.__default_jvm is not None:
            if self.__default_jvm.path is not None:
                print("%s  Default Java executable: %s" % \
                    (prefix, self.__default_jvm.path), file=fd)
            if self.__default_jvm.isServer is not None:
                print("%s  Default Java server flag: %s" % \
                    (prefix, self.__default_jvm.isServer), file=fd)
            if self.__default_jvm.heapInit is not None:
                print("%s  Default Java heap init: %s" % \
                    (prefix, self.__default_jvm.heapInit), file=fd)
            if self.__default_jvm.heapMax is not None:
                print("%s  Default Java heap max: %s" % \
                    (prefix, self.__default_jvm.heapMax), file=fd)
            if self.__default_jvm.args is not None:
                print("%s  Default Java arguments: %s" % \
                    (prefix, self.__default_jvm.args), file=fd)
            if self.__default_jvm.extraArgs is not None:
                print("%s  Default Java extra arguments: %s" % \
                    (prefix, self.__default_jvm.extraArgs), file=fd)

        if self.__default_log_level is not None:
            print("%s  Default log level: %s" % \
                (prefix, self.__default_log_level), file=fd)

        if self.__defaults.Components is None or \
           len(self.__defaults.Components) == 0:
            print("  **No default components**", file=fd)
        else:
            print("  Default components:", file=fd)
            for comp in list(self.__defaults.Components.keys()):
                print("%s    %s:" % (prefix, comp), file=fd)

                if 'hitspoolDirectory' in self.__defaults.Components[comp]:
                    print("%s      HS directory: %s" % \
                        (prefix,
                         self.__defaults.Components[comp]['hitspoolDirectory']), file=fd)
                if 'hitspoolInterval' in self.__defaults.Components[comp]:
                    print("%s      HS interval: %s" % \
                        (prefix,
                         self.__defaults.Components[comp]['hitspoolInterval']), file=fd)
                if 'hitspoolMaxFiles' in self.__defaults.Components[comp]:
                    print("%s      HS max files: %s" % \
                        (prefix,
                         self.__defaults.Components[comp]['hitspoolMaxFiles']), file=fd)

                if 'jvmPath' in self.__defaults.Components[comp]:
                    print("%s      Java executable: %s" % \
                        (prefix, self.__defaults.Components[comp]['jvmPath']), file=fd)
                if 'jvmServer' in self.__defaults.Components[comp]:
                    print("%s      Java server flag: %s" % \
                        (prefix, self.__defaults.Components[comp]['jvmServer']), file=fd)
                if 'jvmHeapInit' in self.__defaults.Components[comp]:
                    print("%s      Java initial heap size: %s" % \
                        (prefix,
                         self.__defaults.Components[comp]['jvmHeapInit']), file=fd)
                if 'jvmHeapMax' in self.__defaults.Components[comp]:
                    print("%s      Java maximum heap size: %s" % \
                        (prefix,
                         self.__defaults.Components[comp]['jvmHeapMax']), file=fd)
                if 'jvmArgs' in self.__defaults.Components[comp]:
                    print("%s      Java arguments: %s" % \
                        (prefix, self.__defaults.Components[comp]['jvmArgs']), file=fd)
                if 'jvmExtraArgs' in self.__defaults.Components[comp]:
                    print("%s      Java extra arguments: %s" % \
                        (prefix,
                         self.__defaults.Components[comp]['jvmExtraArgs']), file=fd)

                if 'logLevel' in self.__defaults.Components[comp]:
                    print("%s      Log level: %s" % \
                        (prefix, self.__defaults.Components[comp]['logLevel']), file=fd)

        if self.__host_map is not None:
            hKeys = sorted(self.__host_map.keys())

            for key in hKeys:
                self.__host_map[key].dump(fd=fd, prefix=prefix + "  ")

    def extractFrom(self, dom):
        "Extract all necessary information from a cluster configuration file"
        cluName = 'cluster'
        kids = dom.getElementsByTagName(cluName)
        if len(kids) < 1:
            raise XMLFormatError('No <%s> node found' % cluName)
        elif len(kids) > 1:
            raise XMLFormatError('Multiple <%s> nodes found' % cluName)

        cluster = kids[0]

        name = self.getValue(cluster, 'name')

        defaults = ClusterDefaults()

        dfltNodes = cluster.getElementsByTagName('default')
        for node in dfltNodes:
            self.__parse_default_nodes(name, defaults, node)

        hostNodes = cluster.getElementsByTagName('host')
        if len(hostNodes) < 1:
            errMsg = 'No hosts defined for cluster "%s"' % name
            raise ClusterDescriptionFormatError(errMsg)

        hostMap = self.__parse_host_nodes(name, defaults, hostNodes)

        self.name = name
        self.__defaults = defaults
        self.__host_map = hostMap

        self.__spade_log_dir = self.getValue(cluster, 'logDirForSpade')
        # expand tilde
        if self.__spade_log_dir is not None:
            self.__spade_log_dir = os.path.expanduser(self.__spade_log_dir)

        self.__log_dir_copies = self.getValue(cluster, 'logDirCopies')
        if self.__log_dir_copies is not None:
            self.__log_dir_copies = os.path.expanduser(self.__log_dir_copies)

        self.__daq_data_dir = self.getValue(cluster, 'daqDataDir')
        if self.__daq_data_dir is not None:
            self.__daq_data_dir = os.path.expanduser(self.__daq_data_dir)

        self.__daq_log_dir = self.getValue(cluster, 'daqLogDir')
        if self.__daq_log_dir is not None:
            self.__daq_log_dir = os.path.expanduser(self.__daq_log_dir)

        self.__pkg_stage_dir = self.getValue(cluster, 'packageStageDir')
        if self.__pkg_stage_dir is not None:
            self.__pkg_stage_dir = os.path.expanduser(self.__pkg_stage_dir)

        self.__pkg_install_dir = self.getValue(cluster, 'packageInstallDir')
        if self.__pkg_install_dir is not None:
            self.__pkg_install_dir = os.path.expanduser(self.__pkg_install_dir)

    @classmethod
    def getClusterName(cls, hostname=None):
        """
        Determine the cluster name from 'hostname'.
        Returned values are "sps", "spts", "spts64", or "localhost".
        If 'hostname' is not set, the host name of the current machine is used.
        """

        mid = Machineid()
        if mid.is_sps_cluster:
            return cls.SPS
        if mid.is_spts_cluster:
            return cls.SPTS
        if mid.is_mdfl_cluster:
            return cls.MDFL

        return cls.LOCAL

    @classmethod
    def getClusterDatabaseType(cls, clu=None):
        """
        Determine the database type for the cluster description.
        'clu' should be one of the ClusterDescription constants
        """
        mid = Machineid()
        if mid.is_spts_cluster:
            dbname = cls.getLiveDBName()
            if dbname is None or dbname == "I3OmDb_test":
                return cls.DBTYPE_TEST
            elif dbname == "I3OmDb":
                return cls.DBTYPE_PROD
            raise NotImplementedError(("Unknown database \"%s\" for" +
                                       " cluster \"%s\"") % (dbname, clu))
        if mid.is_sps_cluster:
            return cls.DBTYPE_PROD
        if mid.is_mdfl_cluster or mid.is_unknown_cluster: 
            return cls.DBTYPE_NONE
        raise NotImplementedError("Cannot guess database" +
                                  " for cluster \"%s\"" % clu)

    @classmethod
    def getLiveDBName(cls):
        liveConfigName = ".i3live.conf"

        path = os.path.join(os.environ["HOME"], liveConfigName)
        if os.path.exists(path):
            with open(path, "r") as fd:
                for line in fd:
                    if line.startswith("["):
                        ridx = line.find("]")
                        if ridx < 0:
                            # ignore line with bad section marker
                            continue

                        section = line[1:ridx]
                        continue

                    if section != "livecontrol":
                        continue

                    pos = line.find("=")
                    if pos < 0:
                        continue

                    if line[:pos].strip() != "dbname":
                        continue

                    return line[pos + 1:].strip()

        return None

    def host(self, name):
        if name not in self.__host_map:
            return None

        return self.__host_map[name]

    @property
    def hosts(self):
        for host in self.__host_map:
            yield host

    def listHostComponentPairs(self):
        for host in list(self.__host_map.keys()):
            for comp in self.__host_map[host].getComponents():
                yield (host, comp)
            if self.__host_map[host].isControlServer:
                yield (host, ControlComponent())

    def listHostSimHubPairs(self):
        for host in list(self.__host_map.keys()):
            if self.__host_map[host].simHubs is not None:
                for sh in self.__host_map[host].simHubs:
                    yield (host, sh)

    @property
    def logDirForSpade(self):
        return self.__spade_log_dir

    @property
    def logDirCopies(self):
        return self.__log_dir_copies

    @property
    def packageStageDir(self):
        if self.__pkg_stage_dir is None:
            return self.DEFAULT_PKGSTAGE_DIR
        return self.__pkg_stage_dir

    @property
    def packageInstallDir(self):
        if self.__pkg_install_dir is None:
            return self.DEFAULT_PKGINSTALL_DIR
        return self.__pkg_install_dir


if __name__ == '__main__':
    def tryCluster(configDir, path=None):
        if path is None:
            cluster = ClusterDescription(configDir)
        else:
            dirName = os.path.dirname(path)
            if dirName is None or len(dirName) == 0:
                dirName = configDir
                baseName = path
            else:
                baseName = os.path.basename(path)

            try:
                cluster = ClusterDescription(dirName, baseName)
            except KeyboardInterrupt:
                return
            except NotImplementedError:
                print('For %s:' % path, file=sys.stderr)
                traceback.print_exc()
                return
            except:
                print('For %s:' % path, file=sys.stderr)
                traceback.print_exc()
                return

        print('Saw description %s' % cluster.name)
        cluster.dump()

    configDir = find_pdaq_config()

    if len(sys.argv) == 1:
        tryCluster(configDir)
    for name in sys.argv[1:]:
        tryCluster(configDir, name)
