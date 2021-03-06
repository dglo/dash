#!/usr/bin/env python

import traceback
import unittest
from CnCLogger import CnCLogger

from DAQMocks import MockLogger, SocketReaderFactory


class CnCLoggerTest(unittest.TestCase):
    "Test CnCLogger class"
    def __create_log(self, name, port):
        "Create a socket reader"
        return self.__log_factory.create_log(name, port, False)

    def setUp(self):
        self.__log_factory = SocketReaderFactory()

        self.__appender = MockLogger("mock")

    def tearDown(self):
        try:
            self.__log_factory.tearDown()
        except:  # pylint: disable=bare-except
            traceback.print_exc()

        self.__appender.check_status(10)

    def test_no_appenders(self):  # pylint: disable=no-self-use
        "Test opening and resetting pDAQ logger"
        clog = CnCLogger("NoAppenders", appender=None, quiet=True)
        # this message will get dropped
        clog.error("Test")

    def test_open_reset(self):
        "Test opening and resetting pDAQ logger"
        dflt_host = "localhost"
        dflt_port = 54321

        dflt_obj = self.__create_log("dflt", dflt_port)

        log_host = "localhost"
        log_port = 12345

        log_obj = self.__create_log("file", log_port)

        for xld in (False, True):
            clog = CnCLogger("xld=%s" % str(xld), appender=self.__appender,
                             quiet=True, extra_loud=xld)

            # set up default logger
            dflt_obj.add_expected_text("Start of log at LOG=log(%s:%d)" %
                                       (dflt_host, dflt_port))
            clog.open_log(dflt_host, dflt_port, None, None)

            # set up file logger
            log_obj.add_expected_text("Start of log at LOG=log(%s:%d)" %
                                      (log_host, log_port))
            clog.open_log(log_host, log_port, None, None)

            self.assertEqual(clog.log_host, log_host)
            self.assertEqual(clog.log_port, log_port)
            self.assertEqual(clog.live_host, None)
            self.assertEqual(clog.live_port, None)

            log_obj.check_status(1000)
            dflt_obj.check_status(1000)

            if xld:
                dflt_obj.add_expected_text("Reset log to LOG=log(%s:%d)" %
                                           (dflt_host, dflt_port))

            clog.reset_log()
            self.assertFalse(clog.live_host is not None,
                             "live_host was not cleared")
            self.assertFalse(clog.live_port is not None,
                             "live_port was not cleared")
            self.assertEqual(clog.log_host, dflt_host,
                             "log_host should be %s, not %s" %
                             (dflt_host, clog.log_host))
            self.assertEqual(clog.log_port, dflt_port,
                             "log_port should be %s, not %s" %
                             (dflt_port, clog.log_port))

            log_obj.check_status(1000)
            dflt_obj.check_status(1000)

    def test_open_reset_live(self):
        "Test opening and resetting Live logger"
        dflt_host = "localhost"
        dflt_port = 54321

        dflt_obj = self.__create_log("dflt", dflt_port)

        log_host = "localhost"
        log_port = 6789

        log_obj = self.__create_log("log", log_port)

        for xld in (False, True):
            clog = CnCLogger("xld=%s" % str(xld), appender=self.__appender,
                             quiet=True, extra_loud=xld)

            dflt_obj.add_expected_text("Start of log at LOG=log(%s:%d)" %
                                       (dflt_host, dflt_port))

            # set up default logger
            clog.open_log(dflt_host, dflt_port, None, None)

            dflt_obj.check_status(1000)
            log_obj.check_status(1000)

            log_obj.add_expected_text("Start of log at LOG=log(%s:%d)" %
                                      (log_host, log_port))

            clog.open_log(log_host, log_port, None, None)
            self.assertEqual(clog.live_host, None)
            self.assertEqual(clog.live_port, None)
            self.assertEqual(clog.log_host, log_host)
            self.assertEqual(clog.log_port, log_port)

            dflt_obj.check_status(1000)
            log_obj.check_status(1000)

            if xld:
                dflt_obj.add_expected_text("Reset log to LOG=log(%s:%d)" %
                                           (dflt_host, dflt_port))

            clog.reset_log()
            self.assertEqual(clog.log_host, dflt_host,
                             "log_host should be %s, not %s" %
                             (dflt_host, clog.log_host))
            self.assertEqual(clog.log_port, dflt_port,
                             "log_port should be %s, not %s" %
                             (dflt_port, clog.log_port))
            self.assertFalse(clog.live_host is not None,
                             "live_host was not cleared")
            self.assertFalse(clog.live_port is not None,
                             "live_port was not cleared")

            log_obj.check_status(1000)
            dflt_obj.check_status(1000)

    def test_open_reset_both(self):
        "Test opening and resetting both pDAQ and Live loggers"
        dflt_host = "localhost"
        dflt_log = 54321
        dflt_live = 9876

        dlog_obj = self.__create_log("dlog", dflt_log)
        dlive_obj = self.__create_log("dlive", dflt_live)

        host = "localhost"
        log_port = 12345
        live_port = 6789

        log_obj = self.__create_log("file", log_port)
        live_obj = self.__create_log("live", live_port)

        for xld in (False, True):
            clog = CnCLogger("xld=%s" % str(xld), appender=self.__appender,
                             quiet=True, extra_loud=xld)

            dlog_obj.add_expected_text("Start of log at LOG=log(%s:%d)"
                                       " live(%s:%d)" %
                                       (dflt_host, dflt_log, dflt_host,
                                        dflt_live))

            # set up default logger
            clog.open_log(dflt_host, dflt_log, dflt_host, dflt_live)

            dlog_obj.check_status(1000)
            dlive_obj.check_status(1000)
            log_obj.check_status(1000)
            live_obj.check_status(1000)

            log_obj.add_expected_text("Start of log at LOG=log(%s:%d)"
                                      " live(%s:%d)" %
                                      (host, log_port, host, live_port))

            clog.open_log(host, log_port, host, live_port)
            self.assertEqual(clog.log_host, host)
            self.assertEqual(clog.log_port, log_port)
            self.assertEqual(clog.live_host, host)
            self.assertEqual(clog.live_port, live_port)

            dlog_obj.check_status(1000)
            dlive_obj.check_status(1000)
            log_obj.check_status(1000)
            live_obj.check_status(1000)

            if xld:
                dlog_obj.add_expected_text("Reset log to LOG=log(%s:%d)"
                                           " live(%s:%d)" %
                                           (dflt_host, dflt_log,
                                            dflt_host, dflt_live))

            clog.reset_log()
            self.assertEqual(clog.log_host, dflt_host,
                             "log_host should be %s, not %s" %
                             (dflt_host, clog.log_host))
            self.assertEqual(clog.log_port, dflt_log,
                             "log_port should be %s, not %s" %
                             (dflt_log, clog.log_port))
            self.assertEqual(clog.live_host, dflt_host,
                             "live_host should be %s, not %s" %
                             (dflt_host, clog.live_host))
            self.assertEqual(clog.live_port, dflt_live,
                             "live_port should be %s, not %s" %
                             (dflt_live, clog.live_port))

            log_obj.check_status(1000)
            live_obj.check_status(1000)
            dlog_obj.check_status(1000)
            dlive_obj.check_status(1000)

    def test_open_close(self):
        "Test opening and closing pDAQ logger"
        dflt_host = "localhost"
        dflt_log = 54321
        dflt_live = 9876

        dlog_obj = self.__create_log("dlog", dflt_log)
        dlive_obj = self.__create_log("dlive", dflt_live)

        log_host = "localhost"
        log_port = 12345

        log_obj = self.__create_log("file", log_port)

        for xld in (False, True):
            clog = CnCLogger("xld=%s" % str(xld), appender=self.__appender,
                             quiet=True, extra_loud=xld)

            dlog_obj.add_expected_text("Start of log at LOG=log(%s:%d)"
                                       " live(%s:%d)" %
                                       (dflt_host, dflt_log,
                                        dflt_host, dflt_live))

            # set up default logger
            clog.open_log(dflt_host, dflt_log, dflt_host, dflt_live)

            dlog_obj.check_status(1000)
            dlive_obj.check_status(1000)

            log_obj.add_expected_text("Start of log at LOG=log(%s:%d)" %
                                      (log_host, log_port))

            clog.open_log(log_host, log_port, None, None)
            self.assertEqual(clog.log_host, log_host)
            self.assertEqual(clog.log_port, log_port)
            self.assertEqual(clog.live_host, None)
            self.assertEqual(clog.live_port, None)

            log_obj.check_status(1000)
            dlog_obj.check_status(1000)
            dlive_obj.check_status(1000)

            if xld:
                log_obj.add_expected_text("End of log")
                dlog_obj.add_expected_text("Reset log to LOG=log(%s:%d)"
                                           " live(%s:%d)" %
                                           (dflt_host, dflt_log,
                                            dflt_host, dflt_live))

            clog.close_log()
            self.assertEqual(clog.log_host, dflt_host,
                             "log_host should be %s, not %s" %
                             (dflt_host, clog.log_host))
            self.assertEqual(clog.log_port, dflt_log,
                             "log_port should be %s, not %s" %
                             (dflt_log, clog.log_port))
            self.assertEqual(clog.live_host, dflt_host,
                             "live_host should be %s, not %s" %
                             (dflt_host, clog.live_host))
            self.assertEqual(clog.live_port, dflt_live,
                             "live_port should be %s, not %s" %
                             (dflt_live, clog.live_port))

            log_obj.check_status(1000)
            dlog_obj.check_status(1000)
            dlive_obj.check_status(1000)

    def test_open_close_live(self):
        "Test opening and closing Live logger"
        dflt_host = "localhost"
        dflt_log = 54321
        dflt_live = 9876

        dlog_obj = self.__create_log("dlog", dflt_log)
        dlive_obj = self.__create_log("dlive", dflt_live)

        live_host = "localhost"
        live_port = 6789

        live_obj = self.__create_log("live", live_port)

        for xld in (False, True):
            clog = CnCLogger("xld=%s" % str(xld), appender=self.__appender,
                             quiet=True, extra_loud=xld)

            dlog_obj.add_expected_text("Start of log at LOG=log(%s:%d)"
                                       " live(%s:%d)" %
                                       (dflt_host, dflt_log,
                                        dflt_host, dflt_live))

            # set up default logger
            clog.open_log(dflt_host, dflt_log, dflt_host, dflt_live)

            dlog_obj.check_status(1000)
            dlive_obj.check_status(1000)

            clog.open_log(None, None, live_host, live_port)
            self.assertEqual(clog.log_host, None)
            self.assertEqual(clog.log_port, None)
            self.assertEqual(clog.live_host, live_host)
            self.assertEqual(clog.live_port, live_port)

            live_obj.check_status(1000)
            dlog_obj.check_status(1000)
            dlive_obj.check_status(1000)

            if xld:
                dlog_obj.add_expected_text("Reset log to LOG=log(%s:%d)"
                                           " live(%s:%d)" %
                                           (dflt_host, dflt_log,
                                            dflt_host, dflt_live))

            clog.close_log()
            self.assertEqual(clog.log_host, dflt_host,
                             "log_host should be %s, not %s" %
                             (dflt_host, clog.log_host))
            self.assertEqual(clog.log_port, dflt_log,
                             "log_port should be %s, not %s" %
                             (dflt_log, clog.log_port))
            self.assertEqual(clog.live_host, dflt_host,
                             "live_host should be %s, not %s" %
                             (dflt_host, clog.live_host))
            self.assertEqual(clog.live_port, dflt_live,
                             "live_port should be %s, not %s" %
                             (dflt_live, clog.live_port))

            live_obj.check_status(1000)
            dlog_obj.check_status(1000)
            dlive_obj.check_status(1000)

    def test_open_close_both(self):
        "Test opening and closing both pDAQ and Live loggers"
        dflt_host = "localhost"
        dflt_log = 54321
        dflt_live = 9876

        dlog_obj = self.__create_log("dlog", dflt_log)
        dlive_obj = self.__create_log("dlive", dflt_live)

        log_host = "localhost"
        log_port = 12345
        live_host = ""
        live_port = 0

        log_obj = self.__create_log("file", log_port)

        for xld in (False, True):
            clog = CnCLogger("xld=%s" % str(xld), appender=self.__appender,
                             quiet=True, extra_loud=xld)

            dlog_obj.add_expected_text("Start of log at LOG=log(%s:%d)"
                                       " live(%s:%d)" %
                                       (dflt_host, dflt_log,
                                        dflt_host, dflt_live))

            # set up default logger
            clog.open_log(dflt_host, dflt_log, dflt_host, dflt_live)

            dlog_obj.check_status(1000)
            dlive_obj.check_status(1000)

            log_obj.add_expected_text("Start of log at LOG=log(%s:%d)" %
                                      (log_host, log_port))

            clog.open_log(log_host, log_port, live_host, live_port)
            self.assertEqual(clog.log_host, log_host)
            self.assertEqual(clog.log_port, log_port)
            self.assertEqual(clog.live_host, live_host)
            self.assertEqual(clog.live_port, live_port)

            if xld:
                log_obj.add_expected_text_regexp("End of log")
                dlog_obj.add_expected_text("Reset log to LOG=log(%s:%d)"
                                           " live(%s:%d)" %
                                           (dflt_host, dflt_log,
                                            dflt_host, dflt_live))

            clog.close_log()
            self.assertEqual(clog.log_host, dflt_host,
                             "log_host should be %s, not %s" %
                             (dflt_host, clog.log_host))
            self.assertEqual(clog.log_port, dflt_log,
                             "log_port should be %s, not %s" %
                             (dflt_log, clog.log_port))
            self.assertEqual(clog.live_host, dflt_host,
                             "live_host should be %s, not %s" %
                             (dflt_host, clog.live_host))
            self.assertEqual(clog.live_port, dflt_live,
                             "live_port should be %s, not %s" %
                             (dflt_live, clog.live_port))

            log_obj.check_status(1000)
            dlog_obj.check_status(1000)
            dlive_obj.check_status(1000)


if __name__ == "__main__":
    unittest.main()
