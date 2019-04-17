#!/usr/bin/env python

from __future__ import print_function

import sys

from DAQLog import DAQLog, LiveSocketAppender, LogException, LogSocketAppender


class LogInfo(object):
    def __init__(self, log_host, log_port, live_host, live_port):
        self.__log_host = log_host
        self.__log_port = log_port
        self.__live_host = live_host
        self.__live_port = live_port

    def __cmp__(self, other):
        val = cmp(self.log_host, other.log_host)
        if val == 0:
            val = cmp(self.log_port, other.log_port)
            if val == 0:
                val = cmp(self.live_host, other.live_host)
                if val == 0:
                    val = cmp(self.live_port, other.live_port)
        return val

    def __str__(self):
        outstr = ''
        if self.__log_host is not None and self.__log_port is not None:
            outstr += ' log(%s:%d)' % (self.__log_host, self.__log_port)
        if self.__live_host is not None and self.__live_port is not None:
            outstr += ' live(%s:%d)' % (self.__live_host, self.__live_port)
        if outstr == "":
            return 'NoInfo'
        return outstr[1:]

    @property
    def live_host(self):
        return self.__live_host

    @property
    def live_port(self):
        return self.__live_port

    @property
    def log_host(self):
        return self.__log_host

    @property
    def log_port(self):
        return self.__log_port


class CnCLogger(DAQLog):
    "CnC logging client"

    def __init__(self, name, appender=None, quiet=False, extra_loud=False):
        "create a logging client"
        self.__quiet = quiet
        self.__extra_loud = extra_loud

        self.__prev_info = None
        self.__log_info = None

        super(CnCLogger, self).__init__(name, appender=appender)

    def __str__(self):
        if self.__log_info is not None:
            return 'LOG=%s' % str(self.__log_info)
        if self.__prev_info is not None:
            return 'PREV=%s' % str(self.__prev_info)
        return '?LOG?'

    def __add_appenders(self):
        if self.__log_info.log_host is not None and \
                self.__log_info.log_port is not None:
            self.add_appender(LogSocketAppender(self.__log_info.log_host,
                                                self.__log_info.log_port))

        if self.__log_info.live_host is not None and \
                self.__log_info.live_port is not None:
            self.add_appender(LiveSocketAppender(self.__log_info.live_host,
                                                 self.__log_info.live_port))
        if not self.has_appender():
            raise LogException("Not logging to socket or I3Live")

    def _logmsg(self, level, msg, retry=True):
        """
        Log a string to stdout and, if available, to the socket logger
        stdout of course will not appear if daemonized.
        """
        if not self.__quiet:
            print(msg)

        try:
            super(CnCLogger, self)._logmsg(level, msg)
        except Exception as ex:
            if not isinstance(ex, LogException):
                if str(ex).find('Connection refused') < 0:
                    raise
                print('Lost logging connection to %s' % \
                      str(self.__log_info), file=sys.stderr)
            self.reset_log()
            if retry and self.has_appender():
                self._logmsg(level, msg, False)

    def close_log(self):
        "Close the log socket"
        if self.has_appender() and self.__extra_loud:
            self.info("End of log")
        self.reset_log()

    def close_final(self):
        self.close()
        self.__log_info = None
        self.__prev_info = None

    @property
    def live_host(self):
        if self.__log_info is None:
            return None
        return self.__log_info.live_host

    @property
    def live_port(self):
        if self.__log_info is None:
            return None
        return self.__log_info.live_port

    @property
    def log_host(self):
        if self.__log_info is None:
            return None
        return self.__log_info.log_host

    @property
    def log_port(self):
        if self.__log_info is None:
            return None
        return self.__log_info.log_port

    def open_log(self, log_host, log_port, live_host, live_port):
        "initialize socket logger"
        if self.__prev_info is None:
            self.__prev_info = self.__log_info

        self.close()

        self.__log_info = LogInfo(log_host, log_port, live_host, live_port)
        self.__add_appenders()

        self.debug('Start of log at %s' % str(self))

    def reset_log(self):
        "close current log and reset to initial state"

        if self.__prev_info is not None and self.__log_info != self.__prev_info:
            self.close()
            self.__log_info = self.__prev_info
            self.__prev_info = None
            self.__add_appenders()

        if self.has_appender() and self.__extra_loud:
            self.info('Reset log to %s' % str(self))
