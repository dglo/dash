#!/usr/bin/env python

from CnCTask import CnCTask, TaskException
from CnCThread import CnCThread
from ComponentManager import listComponentRanges
import sys

from exc_string import exc_string, set_exc_string_encoding
set_exc_string_encoding("ascii")


class UnhealthyRecord(object):
    def __init__(self, msg, order):
        self.__msg = msg
        self.__order = order

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "#%d: %s" % (self.__order, self.__msg)

    def __cmp__(self, other):
        if not isinstance(other, UnhealthyRecord):
            return -1

        val = cmp(self.__order, other.__order)
        if val == 0:
            val = cmp(self.__msg, other.__msg)
        return val

    def message(self):
        return self.__msg

    def order(self):
        return self.__order


class Watcher(object):
    def __init__(self, fullName, beanName, fieldName):
        self.__fullName = fullName
        self.__beanName = beanName
        self.__fieldName = fieldName

    def __repr__(self):
        return self.__fullName

    def __str__(self):
        return self.__fullName

    def beanName(self):
        return self.__beanName

    def fieldName(self):
        return self.__fieldName

    def typeCategory(self, val):
        vType = type(val)
        if vType == tuple:
            return list
        if vType == long:
            return int
        return vType


class ThresholdWatcher(Watcher):
    def __init__(self, comp, beanName, fieldName, threshold, lessThan):
        self.__comp = comp
        self.__threshold = threshold
        self.__lessThan = lessThan

        if self.__lessThan:
            updown = "below"
        else:
            updown = "above"

        fullName = "%s %s.%s %s %s" % \
            (comp.fullname, beanName, fieldName, updown, self.__threshold)
        super(ThresholdWatcher, self).__init__(fullName, beanName, fieldName)

    def __compare(self, threshold, value):
        if self.__lessThan:
            return value < threshold
        else:
            return value > threshold

    def check(self, newValue):
        newType = self.typeCategory(newValue)
        threshType = self.typeCategory(self.__threshold)

        if newType != threshType:
            raise TaskException(("Threshold value for %s is %s, new value" +
                                 " is %s") %
                                (str(self), str(type(self.__threshold)),
                                 str(type(newValue))))
        elif newType == list or newType == dict:
            raise TaskException("ThresholdWatcher does not support %s" %
                                newType)
        elif self.__compare(self.__threshold, newValue):
            return False

        return True

    def unhealthyRecord(self, value):
        if isinstance(value, Exception) and \
                not isinstance(value, TaskException):
            msg = "%s: %s" % (str(self), exc_string())
        else:
            msg = "%s (value=%s)" % (str(self), str(value))
        return UnhealthyRecord(msg, self.__comp.order())


class ValueWatcher(Watcher):
    NUM_UNCHANGED = 3

    def __init__(self, fromComp, toComp, beanName, fieldName):
        self.__fromComp = fromComp
        self.__toComp = toComp
        self.__order = self.__computeOrder(beanName, fieldName)
        self.__prevValue = None
        self.__unchanged = 0

        fullName = "%s->%s %s.%s" % (fromComp.fullname, toComp.fullname,
                                     beanName, fieldName)
        super(ValueWatcher, self).__init__(fullName, beanName, fieldName)

    def __compare(self, oldValue, newValue):
        if newValue < oldValue:
            raise TaskException("%s DECREASED (%s->%s)" %
                                (str(self), str(oldValue), str(newValue)))

        return newValue == oldValue

    def __computeOrder(self, beanName, fieldName):
        if self.__fromComp.isBuilder and self.__toComp.isSource:
            return self.__fromComp.order() + 1

        if self.__fromComp.isSource and self.__toComp.isBuilder:
            return self.__toComp.order() + 2

        return self.__fromComp.order()

    def check(self, newValue):
        if self.__prevValue is None:
            if isinstance(newValue, list):
                self.__prevValue = newValue[:]
            else:
                self.__prevValue = newValue
            return True

        newType = self.typeCategory(newValue)
        prevType = self.typeCategory(self.__prevValue)

        if newType != prevType:
            raise TaskException(("Previous type for %s was %s (%s)," +
                                 " new type is %s (%s)") %
                                (str(self), str(type(self.__prevValue)),
                                 str(self.__prevValue),
                                 str(type(newValue)), str(newValue)))

        if newType == dict:
            raise TaskException("ValueWatcher does not support %s" % newType)

        if newType != list:
            try:
                cmpEq = self.__compare(self.__prevValue, newValue)
            except TaskException:
                self.__unchanged = 0
                raise

            if cmpEq:
                self.__unchanged += 1
                if self.__unchanged == ValueWatcher.NUM_UNCHANGED:
                    raise TaskException(str(self) + " is not changing")
            else:
                self.__unchanged = 0
                self.__prevValue = newValue
        elif len(newValue) != len(self.__prevValue):
            raise TaskException(("Previous %s list had %d entries, new list" +
                                 " has %d") %
                                (str(self), len(self.__prevValue),
                                 len(newValue)))
        else:
            tmpStag = False
            tmpEx = None
            for i in range(len(newValue)):
                try:
                    cmpEq = self.__compare(self.__prevValue[i], newValue[i])
                except TaskException:
                    if not tmpEx:
                        tmpEx = sys.exc_info()
                    cmpEq = False

                if cmpEq:
                    tmpStag = True
                else:
                    self.__prevValue[i] = newValue[i]

            if not tmpStag:
                self.__unchanged = 0
            else:
                self.__unchanged += 1
                if self.__unchanged == ValueWatcher.NUM_UNCHANGED:
                    raise TaskException(("At least one %s value is not" +
                                         " changing") % str(self))

            if tmpEx:
                raise tmpEx[0], tmpEx[1], tmpEx[2]

        return self.__unchanged == 0

    def unhealthyRecord(self, value):
        if isinstance(value, Exception) and \
               not isinstance(value, TaskException):
            msg = "%s: %s" % (str(self), exc_string())
        else:
            msg = "%s not changing from %s" % (str(self),
                                               str(self.__prevValue))
        return UnhealthyRecord(msg, self.__order)


class WatchData(object):
    def __init__(self, comp, mbeanClient, dashlog):
        self.__comp = comp
        self.__mbeanClient = mbeanClient
        self.__dashlog = dashlog

        self.__inputFields = {}
        self.__outputFields = {}
        self.__thresholdFields = {}

        self.__closed = False

    def __str__(self):
        return self.__comp.fullname

    def __checkBeans(self, beanList):
        unhealthy = []
        for b in beanList:
            if self.__closed:
                # break out of the loop if this thread has been closed
                break
            badList = self.__checkValues(beanList[b])
            if badList is not None:
                unhealthy += badList

        if len(unhealthy) == 0:
            return None

        return unhealthy

    def __checkValues(self, watchList):
        unhealthy = []
        if len(watchList) == 1:
            try:
                beanName = watchList[0].beanName()
                fldName = watchList[0].fieldName()
                val = self.__mbeanClient.get(beanName, fldName)

                chkVal = watchList[0].check(val)
            except Exception as ex:
                unhealthy.append(watchList[0].unhealthyRecord(ex))
                chkVal = True
            if not chkVal:
                unhealthy.append(watchList[0].unhealthyRecord(val))
        else:
            beanName = None
            fldList = []
            for f in watchList:
                if beanName is None:
                    beanName = f.beanName()
                elif beanName != f.beanName():
                    self.__dashlog.error("NOT requesting fields from multiple"
                                         " beans (%s != %s)" %
                                         (beanName, f.beanName()))
                    continue
                fldList.append(f.fieldName())

            try:
                valMap = self.__mbeanClient.getAttributes(beanName, fldList)
            except Exception as ex:
                fldList = []
                unhealthy.append(watchList[0].unhealthyRecord(ex))

            for index, fldVal in enumerate(fldList):
                try:
                    val = valMap[fldVal]
                except KeyError:
                    self.__dashlog.error("No value found for %s field#%d %s" %
                                         (self.__comp.fullname, index,
                                          fldVal))
                    continue

                try:
                    chkVal = watchList[index].check(val)
                except Exception as ex:
                    unhealthy.append(watchList[index].unhealthyRecord(ex))
                    chkVal = True
                if not chkVal:
                    unhealthy.append(watchList[index].unhealthyRecord(val))

        if len(unhealthy) == 0:
            return None

        return unhealthy

    def addInputValue(self, otherComp, beanName, fieldName):
        if beanName not in self.__inputFields:
            self.__inputFields[beanName] = []

        vw = ValueWatcher(otherComp, self.__comp, beanName, fieldName)
        self.__inputFields[beanName].append(vw)

    def addOutputValue(self, otherComp, beanName, fieldName):
        if beanName not in self.__outputFields:
            self.__outputFields[beanName] = []

        vw = ValueWatcher(self.__comp, otherComp, beanName, fieldName)
        self.__outputFields[beanName].append(vw)

    def addThresholdValue(self, beanName, fieldName, threshold, lessThan=True):
        """
        Watchdog triggers if field value drops below the threshold value
        (or, when lessThan==False, if value rises above the threshold
        """

        if beanName not in self.__thresholdFields:
            self.__thresholdFields[beanName] = []

        tw = ThresholdWatcher(self.__comp, beanName, fieldName, threshold,
                              lessThan)
        self.__thresholdFields[beanName].append(tw)

    def check(self, starved, stagnant, threshold):
        isOK = True
        if not self.__closed:
            try:
                badList = self.__checkBeans(self.__inputFields)
                if badList is not None:
                    starved += badList
                    isOK = False
            except:
                self.__dashlog.error(self.__comp.fullname + " inputs: " +
                                     exc_string())
                isOK = False

        if not self.__closed and isOK:
            # don't report output problems if there are input problems
            #
            try:
                badList = self.__checkBeans(self.__outputFields)
                if badList is not None:
                    stagnant += badList
                    isOK = False
            except:
                self.__dashlog.error(self.__comp.fullname + " outputs: " +
                                     exc_string())
                isOK = False

        if not self.__closed:
            # report threshold problems even if there are other problems
            #
            try:
                badList = self.__checkBeans(self.__thresholdFields)
                if badList is not None:
                    threshold += badList
                    isOK = False
            except:
                self.__dashlog.error(self.__comp.fullname + " thresholds: " +
                                     exc_string())
                isOK = False

        return isOK

    def close(self):
        self.__closed = True

    def order(self):
        return self.__comp.order()


class WatchdogThread(CnCThread):
    def __init__(self, runset, comp, rule, dashlog, data=None, initFail=0,
                 mbeanClient=None):
        self.__runset = runset
        self.__comp = comp
        if mbeanClient is not None:
            self.__mbeanClient = mbeanClient
        else:
            self.__mbeanClient = comp.createMBeanClient()
        self.__rule = rule
        self.__dashlog = dashlog

        self.__data = data
        self.__initFail = initFail

        self.__starved = []
        self.__stagnant = []
        self.__threshold = []

        super(WatchdogThread, self).__init__(self.__comp.fullname + ":" +
                                             str(self.__rule), self.__dashlog)

    def __str__(self):
        return self.__comp.fullname

    def _run(self):
        if self.isClosed:
            return

        if self.__data is None:
            try:
                self.__data = self.__rule.createData(
                    self.__comp,
                    self.__mbeanClient,
                    self.__runset.components(),
                    self.__dashlog)
            except:
                self.__initFail += 1
                self.__dashlog.error(("Initialization failure #%d" +
                                      " for %s %s: %s") %
                                     (self.__initFail, self.__comp.fullname,
                                      self.__rule, exc_string()))
                return

        self.__data.check(self.__starved, self.__stagnant, self.__threshold)

    def close(self):
        super(WatchdogThread, self).close()

        if self.__data is not None:
            self.__data.close()
            self.__data = None

    def get_new_thread(self):
        thrd = WatchdogThread(self.__runset, self.__comp, self.__rule,
                              self.__dashlog, data=self.__data,
                              initFail=self.__initFail,
                              mbeanClient=self.__mbeanClient)
        return thrd

    def stagnant(self):
        return self.__stagnant[:]

    def starved(self):
        return self.__starved[:]

    def threshold(self):
        return self.__threshold[:]


class DummyComponent(object):
    def __init__(self, name):
        self.__name = name
        self.__order = None

    def __str__(self):
        return self.__name

    @property
    def fullname(self):
        return self.__name

    @property
    def isBuilder(self):
        return False

    @property
    def isSource(self):
        return False

    def order(self):
        return self.__order

    def setOrder(self, num):
        self.__order = num


class WatchdogRule(object):
    DOM_COMP = DummyComponent("dom")
    DISPATCH_COMP = DummyComponent("dispatch")

    def __str__(self):
        return type(self).__name__

    @classmethod
    def findComp(cls, comps, compName):
        for c in comps:
            if c.name == compName:
                return c

        return None

    def initData(self, data, thisComp, components):
        raise NotImplementedError("you were supposed to implement initData")

    def createData(self, thisComp, thisClient, components, dashlog):
        """This is a base class for classes that define initData"""

        data = WatchData(thisComp, thisClient, dashlog)
        self.initData(data, thisComp, components)
        return data

    @classmethod
    def initialize(cls, runset):
        minOrder = None
        maxOrder = None

        for comp in runset.components():
            order = comp.order()
            if not isinstance(order, int):
                raise TaskException("Expected integer order for %s, not %s" %
                                    (comp.fullname, type(comp.order())))

            if minOrder is None or order < minOrder:
                minOrder = order
            if maxOrder is None or order > maxOrder:
                maxOrder = order

        cls.DOM_COMP.setOrder(minOrder - 1)
        cls.DISPATCH_COMP.setOrder(maxOrder + 1)


class StringHubRule(WatchdogRule):
    def initData(self, data, thisComp, components):
        data.addInputValue(self.DOM_COMP, "sender", "NumHitsReceived")
        comp = self.findComp(components, "eventBuilder")
        if comp is not None:
            data.addInputValue(comp, "sender", "NumReadoutRequestsReceived")
            data.addOutputValue(comp, "sender", "NumReadoutsSent")

    def matches(self, comp):
        return comp.name == "stringHub" or comp.name == "replayHub"


class TrackEngineRule(WatchdogRule):
    def initData(self, data, thisComp, components):
        pass

    def matches(self, comp):
        return comp.name == "trackEngine"


class LocalTriggerRule(WatchdogRule):
    def initData(self, data, thisComp, components):
        if thisComp.name == "iceTopTrigger":
            hitName = "icetopHit"
            wantIcetop = True
        else:
            hitName = "stringHit"
            wantIcetop = False

        hub = None
        for comp in components:
            if comp.name.lower().endswith("hub"):
                if wantIcetop:
                    found = comp.num >= 200
                else:
                    found = comp.num < 200
                if found:
                    hub = comp
                    break

        if hub is not None:
            data.addInputValue(hub, hitName, "RecordsReceived")
        comp = self.findComp(components, "globalTrigger")
        if comp is not None:
            data.addOutputValue(comp, "trigger", "RecordsSent")

    def matches(self, comp):
        return comp.name == "inIceTrigger" or \
                   comp.name == "simpleTrigger" or \
                   comp.name == "iceTopTrigger"


class GlobalTriggerRule(WatchdogRule):
    def initData(self, data, thisComp, components):
        for trig in ("inIce", "iceTop", "simple"):
            comp = self.findComp(components, trig + "Trigger")
            if comp is not None:
                data.addInputValue(comp, "trigger", "RecordsReceived")
        comp = self.findComp(components, "eventBuilder")
        if comp is not None:
            data.addOutputValue(comp, "glblTrig", "RecordsSent")

    def matches(self, comp):
        return comp.name == "globalTrigger"


class EventBuilderRule(WatchdogRule):
    def initData(self, data, thisComp, components):
        hub = None
        for comp in components:
            if comp.name.lower().endswith("hub"):
                hub = comp
                break
        if hub is not None:
            data.addInputValue(hub, "backEnd", "NumReadoutsReceived")
        comp = self.findComp(components, "globalTrigger")
        if comp is not None:
            data.addInputValue(comp, "backEnd", "NumTriggerRequestsReceived")
        data.addOutputValue(self.DISPATCH_COMP, "backEnd", "NumEventsSent")
        data.addOutputValue(self.DISPATCH_COMP,
                            "backEnd", "NumEventsDispatched")
        data.addThresholdValue("backEnd", "DiskAvailable", 1024)
        data.addThresholdValue("backEnd", "NumBadEvents", 0, False)

    def matches(self, comp):
        return comp.name == "eventBuilder"


class SecondaryBuildersRule(WatchdogRule):
    def initData(self, data, thisComp, components):
        data.addThresholdValue("snBuilder", "DiskAvailable", 1024)
        data.addOutputValue(self.DISPATCH_COMP, "moniBuilder",
                            "NumDispatchedData")
        data.addOutputValue(self.DISPATCH_COMP, "snBuilder",
                            "NumDispatchedData")
        # XXX - Disabled until there"s a simulated tcal stream
        # data.addOutputValue(self.DISPATCH_COMP, "tcalBuilder",
        #                     "NumDispatchedData")

    def matches(self, comp):
        return comp.name == "secondaryBuilders"


class WatchdogTask(CnCTask):
    NAME = "Watchdog"
    PERIOD = 10

    # number of bad checks before the run is killed
    HEALTH_METER_FULL = 9
    # number of complaints printed before run is killed
    NUM_HEALTH_MSGS = 3

    def __init__(self, taskMgr, runset, dashlog, initial_health=None,
                 period=None, rules=None):
        self.__threadList = {}
        if initial_health is None:
            self.__healthMeter = self.HEALTH_METER_FULL
        else:
            self.__healthMeter = int(initial_health)

        if period is None:
            period = self.PERIOD

        super(WatchdogTask, self).__init__(self.NAME, taskMgr, dashlog,
                                           self.NAME, period)

        WatchdogRule.initialize(runset)

        if rules is None:
            rules = (
                StringHubRule(),
                LocalTriggerRule(),
                TrackEngineRule(),
                GlobalTriggerRule(),
                EventBuilderRule(),
                SecondaryBuildersRule(),
            )

        self.__threadList = self.__createThreads(runset, rules, dashlog)

    def __createThreads(self, runset, rules, dashlog):
        threadList = {}

        components = runset.components()
        for comp in components:
            try:
                found = False
                for rule in rules:
                    if rule.matches(comp):
                        threadList[comp] = self.createThread(runset, comp,
                                                             rule, dashlog)
                        found = True
                        break
                if not found:
                    self.logError("Couldn't create watcher for unknown" +
                                  " component " + comp.fullname)
            except:
                self.logError("Couldn't create watcher for component %s: %s" %
                              (comp.fullname, exc_string()))
        return threadList

    def __logUnhealthy(self, errType, badList):
        errStr = None

        badList.sort()
        for bad in badList:
            if errStr is None:
                errStr = ""
            else:
                errStr += "\n"
            if isinstance(bad, UnhealthyRecord):
                msg = bad.message()
            else:
                msg = "%s (%s is not UnhealthyRecord)" % (str(bad), type(bad))
            errStr += "    " + msg

        self.logError("%s reports %s components:\n%s" %
                      (self.NAME, errType, errStr))

    def _check(self):
        hanging = []
        starved = []
        stagnant = []
        threshold = []

        for c in self.__threadList.keys():
            if self.__threadList[c].isAlive():
                hanging.append(c)
            else:
                starved += self.__threadList[c].starved()
                stagnant += self.__threadList[c].stagnant()
                threshold += self.__threadList[c].threshold()

            self.__threadList[c] = self.__threadList[c].get_new_thread()
            self.__threadList[c].start()

        # watchdog starts out "extra healthy" to compensate for
        #  laggy components at the start of each run
        extra_healthy = self.__healthMeter > self.HEALTH_METER_FULL

        healthy = True
        if len(hanging) > 0:
            if not extra_healthy:
                self.logError("%s reports hanging components:\n    %s" %
                              (self.NAME, listComponentRanges(hanging)))
            healthy = False
        if len(starved) > 0:
            if not extra_healthy:
                self.__logUnhealthy("starved", starved)
            healthy = False
        if len(stagnant) > 0:
            if not extra_healthy:
                self.__logUnhealthy("stagnant", stagnant)
            healthy = False
        if len(threshold) > 0:
            if not extra_healthy:
                self.__logUnhealthy("threshold", threshold)
            healthy = False

        if healthy:
            if not extra_healthy:
                if self.__healthMeter + self.NUM_HEALTH_MSGS < \
                   self.HEALTH_METER_FULL:
                    # only log this if we've logged the "unhealthy" message
                    self.logError("Run is healthy again")
                self.__healthMeter = self.HEALTH_METER_FULL
        else:
            self.__healthMeter -= 1
            if self.__healthMeter > 0:
                if not extra_healthy and \
                   self.__healthMeter % self.NUM_HEALTH_MSGS == 0:
                    self.logError("Run is unhealthy (%d checks left)" %
                                  self.__healthMeter)
            else:
                self.logError("Run is not healthy, stopping")
                self.setError("WatchdogTask")

    @classmethod
    def createThread(cls, runset, comp, rule, dashlog):
        return WatchdogThread(runset, comp, rule, dashlog)

    def close(self):
        savedEx = None
        for thr in self.__threadList.values():
            try:
                thr.close()
            except:
                if not savedEx:
                    savedEx = sys.exc_info()

        if savedEx:
            raise savedEx[0], savedEx[1], savedEx[2]

    def waitUntilFinished(self):
        for c in self.__threadList.keys():
            if self.__threadList[c].isAlive():
                self.__threadList[c].join()
