#!/usr/bin/env python

import calendar
import time
import unittest
from DAQTime import DAQDateTime, PayloadTime

class TestDAQTime(unittest.TestCase):
    TICKS_PER_SEC = 10000000000
    CUR_YEAR = None

    def __checkCompare(self, dt0, dt1, expResult):
        result = cmp(dt0, dt1)
        self.assertEqual(expResult, result,
                         "Expected cmp(%s, %s) to return %s, not %s" %
                         (dt0, dt1, expResult, result))

        result = cmp(dt1, dt0)
        self.assertEqual(-expResult, result,
                         "Expected inverted cmp(%s, %s) to return %s, not %s" %
                         (dt1, dt0, -expResult, result))

    def __dateFormat(self, yr, mon, day, hr, min, sec, usec,
                     high_precision=False):
        if high_precision:
            subsecstr = "%010d" % usec
        else:
            subsecstr = "%06d" % (usec / 10000L)
        return "%04d-%02d-%02d %02d:%02d:%02d.%s" % \
            (yr, mon, day, hr, min, sec, subsecstr)

    def __deltaFormat(self, days, hrs, min, sec, usec):
        rtnstr = "%d day" % days
        if days != 1:
            rtnstr += "s"
        rtnstr += ", %d:%02d:%02d" % (hrs, min, sec)
        if usec > 0:
            rtnstr += ".%06d" % usec
        return rtnstr

    def setUp(self):
        if self.CUR_YEAR is None:
            now = time.gmtime()
            self.CUR_YEAR = now.tm_year

    def testPayloadTimeNone(self):
        self.assertEqual(PayloadTime.toDateTime(None), None)

    def testPayloadTimeZero(self):
        dt = PayloadTime.toDateTime(0)
        expStr = self.__dateFormat(self.CUR_YEAR, 1, 1, 0, 0, 0, 0)
        self.assertEqual(expStr, str(dt),
                         "Expected date %s, not %s" % (expStr, dt))

    def testPayloadTimeZeroHP(self):
        dt = PayloadTime.toDateTime(0, high_precision=True)
        expStr = self.__dateFormat(self.CUR_YEAR, 1, 1, 0, 0, 0, 0,
                                   high_precision=True)
        self.assertEqual(expStr, str(dt),
                         "Expected date %s, not %s" % (expStr, dt))

    def testPayloadTimeOneSec(self):
        dt = PayloadTime.toDateTime(self.TICKS_PER_SEC)
        expStr = self.__dateFormat(self.CUR_YEAR, 1, 1, 0, 0, 1, 0)
        self.assertEqual(expStr, str(dt),
                         "Expected date %s, not %s" % (expStr, dt))

    def testPayloadTimeOneSecHP(self):
        dt = PayloadTime.toDateTime(self.TICKS_PER_SEC, high_precision=True)
        expStr = self.__dateFormat(self.CUR_YEAR, 1, 1, 0, 0, 1, 0,
                                   high_precision=True)
        self.assertEqual(expStr, str(dt),
                         "Expected date %s, not %s" % (expStr, dt))

    def testPayloadTimeOneYear(self):
        jan1 = time.struct_time((self.CUR_YEAR, 1, 1, 0, 0, 0, 0, 0, -1))
        dec31 = time.struct_time((self.CUR_YEAR, 12, 31, 23, 59, 59, 0, 0, -1))
        yrsecs = long(calendar.timegm(dec31) - calendar.timegm(jan1))
        yrticks = yrsecs * self.TICKS_PER_SEC + (self.TICKS_PER_SEC - 10000)
        dt = PayloadTime.toDateTime(yrticks)
        expStr = self.__dateFormat(self.CUR_YEAR, 12, 31, 23, 59, 59, 9999990000)
        self.assertEqual(expStr, str(dt),
                         "Expected date %s, not %s" % (expStr, dt))

    def testPayloadTimeOneYearHP(self):
        jan1 = time.struct_time((self.CUR_YEAR, 1, 1, 0, 0, 0, 0, 0, -1))
        dec31 = time.struct_time((self.CUR_YEAR, 12, 31, 23, 59, 59, 0, 0, -1))
        yrsecs = long(calendar.timegm(dec31) - calendar.timegm(jan1))
        yrticks = yrsecs * self.TICKS_PER_SEC + (self.TICKS_PER_SEC - 10000)
        dt = PayloadTime.toDateTime(yrticks, high_precision=True)
        expStr = self.__dateFormat(self.CUR_YEAR, 12, 31, 23, 59, 59, 9999990000,
                                   high_precision=True)
        self.assertEqual(expStr, str(dt),
                         "Expected date %s, not %s" % (expStr, dt))

    def testDeltaOneDay(self):
        jan1 = time.struct_time((self.CUR_YEAR, 1, 1, 0, 0, 0, 0, 0, -1))
        jan2 = time.struct_time((self.CUR_YEAR, 1, 2, 0, 0, 0, 0, 0, -1))
        dayticks = long(calendar.timegm(jan2) - calendar.timegm(jan1)) * \
            self.TICKS_PER_SEC

        dt0 = PayloadTime.toDateTime(0)
        dt1 = PayloadTime.toDateTime(dayticks)

        expStr = self.__deltaFormat(1, 0, 0, 0, 0)
        self.assertEqual(expStr, str(dt1 - dt0),
                         "Expected delta %s, not %s" % (expStr, dt1 - dt0))

        expStr = self.__deltaFormat(-1, 0, 0, 0, 0)
        self.assertEqual(expStr, str(dt0 - dt1),
                         "Expected delta2 %s, not %s" % (expStr, dt0 - dt1))

    def testDeltaTwoWeeks(self):
        jan1 = time.struct_time((self.CUR_YEAR, 1, 1, 0, 0, 0, 0, 0, -1))
        jan15 = time.struct_time((self.CUR_YEAR, 1, 15, 3, 2, 1, 0, 0, -1))
        usec = 101100
        dayticks = long(calendar.timegm(jan15) - calendar.timegm(jan1)) * \
            self.TICKS_PER_SEC + (usec * 100)

        dt0 = PayloadTime.toDateTime(0)
        dt1 = PayloadTime.toDateTime(dayticks)

        expStr = self.__deltaFormat(14, 3, 2, 1, usec)
        self.assertEqual(expStr, str(dt1 - dt0),
                         "Expected delta %s, not %s" % (expStr, dt1 - dt0))

        expStr = self.__deltaFormat(-15, 20, 57, 58, 99898900)
        self.assertEqual(expStr, str(dt0 - dt1),
                         "Expected delta2 %s, not %s" % (expStr, dt0 - dt1))

    def testDeltaTwoWeeksHP(self):
        jan1 = time.struct_time((self.CUR_YEAR, 1, 1, 0, 0, 0, 0, 0, -1))
        jan15 = time.struct_time((self.CUR_YEAR, 1, 15, 3, 2, 1, 0, 0, -1))
        usec = 101100
        dayticks = long(calendar.timegm(jan15) - calendar.timegm(jan1)) * \
            self.TICKS_PER_SEC + (usec * 100)

        dt0 = PayloadTime.toDateTime(0, high_precision=True)
        dt1 = PayloadTime.toDateTime(dayticks, high_precision=True)

        expStr = self.__deltaFormat(14, 3, 2, 1, usec)
        self.assertEqual(expStr, str(dt1 - dt0),
                         "Expected delta %s, not %s" % (expStr, dt1 - dt0))

        expStr = self.__deltaFormat(-15, 20, 57, 58, 99898900)
        self.assertEqual(expStr, str(dt0 - dt1),
                         "Expected delta2 %s, not %s" % (expStr, dt0 - dt1))

    def testRepr(self):
        expStr = "DAQDateTime(2012, 1, 10, 10, 19, 23, 987654321)"
        dt = eval(expStr)
        shortStr = expStr[0:-5]+"0000)"
        self.assertEqual(shortStr, repr(dt),
                         "Expected repr %s, not %s" % (shortStr, repr(dt)))

    def testReprHP(self):
        expStr = "DAQDateTime(2012, 1, 10, 10, 19, 23, 987654321," + \
            " high_precision=True)"
        dt = eval(expStr)
        self.assertEqual(expStr, repr(dt),
                         "Expected repr %s, not %s" % (expStr, repr(dt)))

    def testCompareNone(self):
        dt0 = DAQDateTime(2012, 1, 10, 10, 19, 23, 987654321)
        dt1 = None
        self.__checkCompare(dt0, dt1, -1)

    def testCompareEqual(self):
        dt0 = DAQDateTime(2012, 1, 10, 10, 19, 23, 987654321)
        dt1 = DAQDateTime(2012, 1, 10, 10, 19, 23, 987654321)
        self.__checkCompare(dt0, dt1, 0)

    def testCompareDiffer(self):
        dt0 = DAQDateTime(2012, 1, 10, 10, 19, 23, 987654321)
        dt1 = DAQDateTime(2011, 1, 1, 1, 1, 1, 0)
        self.__checkCompare(dt0, dt1, 1)

    def testFromStringNone(self):
        self.assertEqual(PayloadTime.fromString(None), None)

    def testFromString(self):
        expStr = "DAQDateTime(2012, 1, 10, 10, 19, 23, 987654321)"
        dt0 = eval(expStr)
        dt1 = PayloadTime.fromString(str(dt0))
        self.__checkCompare(dt0, dt1, 0)

    def testCompareStringEqual(self):
        base = "2012-01-10 10:19:23"

        dt0 = PayloadTime.fromString(base + ".0001000000")
        dt1 = PayloadTime.fromString(base + ".0001")
        self.__checkCompare(dt0, dt1, 0)

        dt0 = PayloadTime.fromString(base + ".0001000000")
        dt1 = PayloadTime.fromString(base + ".00010000")
        self.__checkCompare(dt0, dt1, 0)

        dt0 = PayloadTime.fromString(base + ".0000000000")
        dt1 = PayloadTime.fromString(base)
        self.__checkCompare(dt0, dt1, 0)

    def testCompareStringDiffer(self):
        base = "2012-01-10 10:19:23"
        dt0 = PayloadTime.fromString(base + ".000001")
        dt1 = PayloadTime.fromString(base + ".0")
        self.__checkCompare(dt0, dt1, 1)

    def testCompareStringDifferHP(self):
        base = "2012-01-10 10:19:23"
        dt0 = PayloadTime.fromString(base + ".0000000001", high_precision=True)
        dt1 = PayloadTime.fromString(base + ".0", high_precision=True)
        self.__checkCompare(dt0, dt1, 1)

    def testCompareStringDifferMixed(self):
        base = "2012-01-10 10:19:23"
        dt0 = PayloadTime.fromString(base + ".0000000001", high_precision=True)
        dt1 = PayloadTime.fromString(base + ".0")
        self.__checkCompare(dt0, dt1, 1)

if __name__ == '__main__':
    unittest.main()
