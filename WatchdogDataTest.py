#!/usr/bin/env python

import unittest

from WatchdogTask import UnhealthyRecord, WatchData


class MockBean(object):
    def __init__(self, val):
        self.__val = val

    def nextValue(self):
        raise NotImplementedError()

    def _setValue(self, newVal):
        self.__val = newVal

    def _value(self):
        return self.__val


class MockBeanDecreasing(MockBean):
    def __init__(self, val, dec=1):
        self.__dec = dec
        super(MockBeanDecreasing, self).__init__(val)

    def nextValue(self):
        newVal = self._value() - self.__dec
        self._setValue(newVal)
        return newVal


class MockBeanIncreasing(MockBean):
    def __init__(self, val, inc=1):
        self.__inc = inc
        super(MockBeanIncreasing, self).__init__(val)

    def nextValue(self):
        newVal = self._value() + self.__inc
        self._setValue(newVal)
        return newVal


class MockBeanStagnant(MockBean):
    def __init__(self, val, countDown):
        self.__countDown = countDown
        super(MockBeanStagnant, self).__init__(val)

    def nextValue(self):
        val = self._value()
        if self.__countDown == 0:
            return val
        self.__countDown -= 1
        val += 1
        self._setValue(val)
        return val


class MockBeanTimeBomb(MockBeanIncreasing):
    def __init__(self, val, inc, bombTicks):
        self.__bombTicks = bombTicks
        super(MockBeanTimeBomb, self).__init__(val, inc)

    def nextValue(self):
        if self.__bombTicks == 0:
            raise Exception("TimeBomb")
        self.__bombTicks -= 1
        return super(MockBeanTimeBomb, self).nextValue()


class MockMBeanClient(object):
    def __init__(self):
        self.__beanData = {}

    def __checkAddBean(self, name, fldName):
        if not name in self.__beanData:
            self.__beanData[name] = {}
        if fldName in self.__beanData[name]:
            raise Exception("Cannot add duplicate bean %s.%s to %s" %
                            (name, fldName, self.fullname))

    def addDecreasing(self, name, fldName, val, dec):
        self.__checkAddBean(name, fldName)
        self.__beanData[name][fldName] = MockBeanDecreasing(val, dec)

    def addIncreasing(self, name, fldName, val, inc):
        self.__checkAddBean(name, fldName)
        self.__beanData[name][fldName] = MockBeanIncreasing(val, inc)

    def addStagnant(self, name, fldName, val, countDown):
        self.__checkAddBean(name, fldName)
        self.__beanData[name][fldName] = MockBeanStagnant(val, countDown)

    def addTimeBomb(self, name, fldName, val, inc, bombTicks):
        self.__checkAddBean(name, fldName)
        self.__beanData[name][fldName] = MockBeanTimeBomb(val, inc, bombTicks)

    def check(self, name, fldName):
        if not name in self.__beanData or \
           not fldName in self.__beanData[name]:
            raise Exception("Unknown %s bean %s.%s" %
                            (self.fullname, name, fldName))

    def get(self, beanName, fldName):
        self.check(beanName, fldName)
        return self.__beanData[beanName][fldName].nextValue()

    def getAttributes(self, beanName, fldList):
        rtnMap = {}
        for f in fldList:
            rtnMap[f] = self.get(beanName, f)
        return rtnMap


class MockComponent(object):
    def __init__(self, name, num, order, mbeanClient, source=False,
                 builder=False):
        self.__name = name
        self.__num = num
        self.__order = order
        self.__mbeanClient = mbeanClient
        self.__source = source
        self.__builder = builder

    def __str__(self):
        return self.fullname

    def createMBeanClient(self):
        return self.__mbeanClient

    @property
    def fullname(self):
        if self.__num == 0:
            return self.__name
        return self.__name + "#%d" % self.__num

    @property
    def isBuilder(self):
        return self.__builder

    @property
    def isSource(self):
        return self.__source

    def order(self):
        return self.__order


class WatchdogDataTest(unittest.TestCase):
    def testCreate(self):
        comp = MockComponent("foo", 1, 1, MockMBeanClient())

        wd = WatchData(comp, None)
        self.assertEqual(comp.order(), wd.order(),
                         "Expected WatchData order %d, not %d" %
                         (comp.order(), wd.order()))

    def testCheckValuesGood(self):
        beanName = "bean"
        inName = "inFld"
        outName = "outFld"
        ltName = "ltFld"
        gtName = "gtFld"

        threshVal = 15

        mbeanClient = MockMBeanClient()
        mbeanClient.addIncreasing(beanName, inName, 12, 1)
        mbeanClient.addIncreasing(beanName, outName, 5, 1)
        mbeanClient.addIncreasing(beanName, ltName, threshVal, 1)
        mbeanClient.addDecreasing(beanName, gtName, threshVal, 1)

        comp = MockComponent("foo", 1, 1, mbeanClient)
        other = MockComponent("other", 0, 17, MockMBeanClient())

        wd = WatchData(comp, None)

        wd.addInputValue(other, beanName, inName)
        wd.addOutputValue(other, beanName, outName)
        wd.addThresholdValue(beanName, ltName, threshVal, True)
        wd.addThresholdValue(beanName, gtName, threshVal, False)

        starved = []
        stagnant = []
        threshold = []
        for i in range(4):
            if not wd.check(starved, stagnant, threshold):
                self.fail("Check #%d failed" % i)
            self.assertEqual(0, len(starved),
                             "Check #%d returned %d starved (%s)" %
                             (i, len(starved), starved))
            self.assertEqual(0, len(stagnant),
                             "Check #%d returned %d stagnant (%s)" %
                             (i, len(stagnant), stagnant))
            self.assertEqual(0, len(threshold),
                             "Check #%d returned %d threshold (%s)" %
                             (i, len(threshold), threshold))

    def testCheckValuesFailOne(self):
        beanName = "bean"
        inName = "inFld"
        outName = "outFld"
        gtName = "gtFld"

        starveVal = 12
        stagnantVal = 5
        threshVal = 15
        failNum = 2

        for f in range(2):
            mbeanClient = MockMBeanClient()

            comp = MockComponent("foo", 1, 1, mbeanClient)
            other = MockComponent("other", 0, 17, MockMBeanClient())

            wd = WatchData(comp, None)

            if f == 0:
                mbeanClient.addStagnant(beanName, inName, starveVal, failNum)
                wd.addInputValue(other, beanName, inName)
            elif f == 1:
                mbeanClient.addStagnant(beanName, outName, stagnantVal, failNum)
                wd.addOutputValue(other, beanName, outName)

            mbeanClient.addIncreasing(beanName, gtName, threshVal - failNum, 1)
            wd.addThresholdValue(beanName, gtName, threshVal, False)

            for i in range(5):
                starved = []
                stagnant = []
                threshold = []
                rtnval = wd.check(starved, stagnant, threshold)
                #print "\nRTN %s STV %s STG %s THR %s" % \
                #      (rtnval, starved, stagnant, threshold)

                nStarved = 0
                nStagnant = 0
                nThreshold = 0

                if i < failNum:
                    self.assertTrue(rtnval, "Check #%d failed" % i)
                else:
                    self.assertTrue(not rtnval, "Check #%d succeeded" % i)
                    if f == 0:
                        nStarved = 1
                        nStagnant = 0
                    else:
                        nStarved = 0
                        nStagnant = 1
                    nThreshold = 1

                self.assertEqual(nStarved, len(starved),
                                 "Check #%d returned %d starved (%s)" %
                                 (i, len(starved), starved))
                self.assertEqual(nStagnant, len(stagnant),
                                 "Check #%d returned %d stagnant (%s)" %
                                 (i, len(stagnant), stagnant))
                self.assertEqual(nThreshold, len(threshold),
                                 "Check #%d returned %d threshold (%s)" %
                                 (i, len(threshold), threshold))

                if nStarved > 0:
                    msg = UnhealthyRecord(
                        ("%s->%s %s.%s not changing from %d") %
                        (other, comp, beanName, inName,
                         starveVal + failNum), other.order())
                    self.assertEqual(msg, starved[0],
                                     ("Check #%d starved#1 should be" +
                                      " \"%s\" not \"%s\"") %
                                     (i, msg, starved[0]))

                if nStagnant > 0:
                    msg = UnhealthyRecord(("%s->%s %s.%s not changing" +
                                           " from %d") %
                                          (comp, other, beanName, outName,
                                           stagnantVal + failNum),
                                          comp.order())
                    self.assertEqual(msg, stagnant[0],
                                     ("Check #%d stagnant#1 should be" +
                                      " \"%s\" not \"%s\"") %
                                     (i, msg, stagnant[0]))

                if nThreshold > 0:
                    msg = UnhealthyRecord("%s %s.%s above %d (value=%d)" %
                                          (comp, beanName, gtName,
                                           threshVal,
                                           threshVal + i - (failNum - 1)),
                                          comp.order())
                    self.assertEqual(msg, threshold[0],
                                     ("Check #%d threshold#1 should be" +
                                      " \"%s\" not \"%s\"") %
                                     (i, msg, threshold[0]))

    def testCheckValuesTimeBomb(self):
        beanName = "bean"
        inName = "inFld"
        outName = "outFld"
        gtName = "gtFld"

        tVal = 10
        ltThresh = True
        bombTicks = 2

        for f in range(3):
            mbeanClient = MockMBeanClient()

            comp = MockComponent("foo", 1, 1, mbeanClient)
            other = MockComponent("other", 0, 17, MockMBeanClient())

            wd = WatchData(comp, None)

            if f == 0:
                mbeanClient.addTimeBomb(beanName, inName, tVal, 1, bombTicks)
                wd.addInputValue(other, beanName, inName)
            elif f == 1:
                mbeanClient.addTimeBomb(beanName, outName, tVal, 1, bombTicks)
                wd.addOutputValue(other, beanName, outName)
            elif f == 2:
                mbeanClient.addTimeBomb(beanName, gtName, tVal, 1, bombTicks)
                wd.addThresholdValue(beanName, gtName, tVal, ltThresh)

            for i in range(bombTicks + 1):
                starved = []
                stagnant = []
                threshold = []
                rtnval = wd.check(starved, stagnant, threshold)

                nStarved = 0
                nStagnant = 0
                nThreshold = 0

                if i < bombTicks:
                    self.assertTrue(rtnval, "Check #%d failed" % i)
                else:
                    self.assertTrue(not rtnval, "Check #%d succeeded" % i)
                    if f == 0:
                        nStarved = 1
                    elif f == 1:
                        nStagnant = 1
                    elif f == 2:
                        nThreshold = 1

                self.assertEqual(nStarved, len(starved),
                                 "Check #%d returned %d starved (%s)" %
                                 (i, len(starved), starved))
                self.assertEqual(nStagnant, len(stagnant),
                                 "Check #%d returned %d stagnant (%s)" %
                                 (i, len(stagnant), stagnant))
                self.assertEqual(nThreshold, len(threshold),
                                 "Check #%d returned %d threshold (%s)" %
                                 (i, len(threshold), threshold))

                front = None
                badRec = None

                if nStarved > 0:
                    front = "%s->%s %s.%s" % (other, comp, beanName, inName)
                    badRec = starved[0]
                elif nStagnant > 0:
                    front = "%s->%s %s.%s" % (comp, other, beanName, outName)
                    badRec = stagnant[0]
                elif nThreshold > 0:
                    front = "%s %s.%s %s %s" % \
                            (comp, beanName, gtName,
                             ltThresh and "below" or "above", tVal)
                    badRec = threshold[0]

                if front is not None:
                    self.assertTrue(badRec is not None,
                                    "No UnhealthyRecord found for " + front)

                    front += ': Exception("TimeBomb")'
                    if badRec.message().find(front) != 0:
                        self.fail(("Expected UnhealthyRecord %s to start" +
                                   " with \"%s\"") % (badRec, front))

if __name__ == '__main__':
    unittest.main()
