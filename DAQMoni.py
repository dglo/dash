#!/usr/bin/env python

#
# DAQ Monitoring object for high level DAQRun scrupt
#
# John Jacobsen, jacobsen@npxdesigns.com
# Started December, 2006

from DAQLog import *
from DAQRPC import RPCClient
import datetime, sys, threading
from exc_string import *

class MoniData(object):
    def __init__(self, id, fname, name, daqID, addr, port):
        self.id = id
        self.name = name
        self.daqID = daqID
        self.addr = addr
        self.port = port
        if fname is None:
            self.fd = sys.stdout
        else:
            self.fd = open(fname, "w+") # Might throw exception
        self.client = RPCClient(addr, port)
        self.beanFields = {}
        self.beanList = self.client.mbean.listMBeans()
        for bean in self.beanList:
            self.beanFields[bean] = self.client.mbean.listGetters(bean)

    def __str__(self):
        return '%s-%d' % (self.name, self.daqID)

    def unFixValue(self, obj):

        """ Look for longs masquerading as strings ending in an 'L'.
        If a obj is such a string, return it converted to a long.  If
        obj is a dict or list, recuse into it converting all such
        masquerading strings to long.  All other types are
        unaltered.  This pairs with the similarly name fix* methods in
        icecube.daq.juggler.mbean.XMLRPCServer """

        print "ksb - unFixValue: %s" % (str(obj))
        if type(obj) is dict:
            for k in obj.keys():
                obj[k] = self.unFixValue(obj[k])
            return obj
        elif type(obj) is list:
            for i in xrange(0, len(obj)):
                obj[i] = self.unFixValue(obj[i])
            return obj
        elif type(obj) is str and obj[-1] == 'L':
            try:
                return long(obj[:-1])
            except ValueError:
                pass
            return obj
        else:
            return obj

    def monitor(self, now):
        for b in self.beanFields.keys():
            map = self.client.mbean.getAttributes(b, self.beanFields[b])

            # report monitoring data
            if len(map) > 0:
                print >>self.fd, '%s: %s:' % (b, now)
                for key in map:
                    print >>self.fd, '\t%s: %s' % \
                            (key, str(self.unFixValue(map[key])))
            print >>self.fd
            self.fd.flush()

class BeanFieldNotFoundException(Exception): pass

class MoniThread(threading.Thread):
    def __init__(self, moniData, log):
        self.moniData = moniData
        self.log = log

        self.now = None
        self.done = True

        threading.Thread.__init__(self)

        self.setName(str(self.moniData))

    def getNewThread(self, now):
        mt = MoniThread(self.moniData, self.log)
        mt.now = now
        return mt

    def logmsg(self, m):
        "Log message to logger, but only if logger exists"
        print m
        if self.log: self.log.dashLog(m)
     
    def run(self):
        done = False
        try:
            self.moniData.monitor(self.now)
        except Exception:
            self.logmsg("Ignoring %s: %s" % \
                            (str(self.moniData), exc_string()))
        done = True

class DAQMoni(object):
    def __init__(self, daqLog, interval, IDs, shortNameOf, daqIDof, rpcAddrOf, mbeanPortOf):
        self.log         = daqLog
        self.path        = daqLog.logPath
        self.interval    = interval
        self.tstart      = datetime.datetime.now()
        self.tlast       = None
        self.moniList    = {}
        self.threadList  = {}
        for c in IDs:
            if mbeanPortOf[c] > 0:
                fname = DAQMoni.fileName(self.path, shortNameOf[c], daqIDof[c])
                self.logmsg("Creating moni output file %s (remote is %s:%d)" % (fname,
                                                                                rpcAddrOf[c],
                                                                                mbeanPortOf[c]))
                try:
                    md = MoniData(c, fname, shortNameOf[c], daqIDof[c], rpcAddrOf[c], mbeanPortOf[c])
                    self.moniList[c] = md
                    self.threadList[c] = MoniThread(md, self.log)
                except Exception, e:
                    self.logmsg("Couldn't create monitoring output (%s) for component %d!" % (fname, c))
                    self.logmsg("%s: %s" % (e, exc_string()))

    def fileName(path, name, daqID):
        return "%s/%s-%d.moni" % (path, name, daqID)
    fileName = staticmethod(fileName)

    def getSingleBeanField(self, ID, beanName, beanField):
        if not self.moniList:
            raise BeanFieldNotFoundException("Empty list of monitoring objects")
        if ID not in self.moniList:
            raise BeanFieldNotFoundException("Component %d not found" % ID)
        md = self.moniList[ID]
        if beanName not in md.beanList:
            raise BeanFieldNotFoundException("Bean %s not in list of beans for ID %d" % (beanName, ID))

        if beanField not in md.beanFields[beanName]:
            raise BeanFieldNotFoundException("Bean field %s not in list of bean fields (%s) for bean %s"
                                             % (beanField, `md.beanFields`, beanName))
        return md.client.mbean.get(beanName, beanField)
    
    def timeToMoni(self):
        if not self.tlast: return True
        now = datetime.datetime.now()
        dt  = now - self.tlast
        if dt.seconds+dt.microseconds*1.E-6 > self.interval: return True
        return False
    
    def doMoni(self):
        now = datetime.datetime.now()
        self.tlast = now
        for c in self.threadList.keys():
            if self.threadList[c].done:
                self.threadList[c] = self.threadList[c].getNewThread(now)
                self.threadList[c].start()

    def logmsg(self, m):
        "Log message to logger, but only if logger exists"
        print m
        if self.log: self.log.dashLog(m)
     
if __name__ == "__main__":
    usage = False
    if len(sys.argv) < 2:
        usage = True
    else:
        for i in range(1,len(sys.argv)):
            colon = sys.argv[i].find(':')
            if colon < 0:
                print "No colon"
                usage = True
            else:
                host = sys.argv[i][:colon]
                port = sys.argv[i][colon+1:]

                moni = MoniData(i, None, 'unknown', 0, host, port)
                moni.monitor('snapshot')
    if usage:
        print "Usage: DAQMoni.py host:beanPort [host:beanPort ...]"
        raise SystemExit

