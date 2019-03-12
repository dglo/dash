#!/usr/bin/env python

import unittest
from DAQClient import DAQClient

from DAQMocks import MockCnCLogger, MockLogger


class MostlyDAQClient(DAQClient):
    def __init__(self, name, num, host, port, mbeanPort, connectors, appender):
        self.__appender = appender

        super(MostlyDAQClient, self).__init__(name, num, host, port,
                                              mbeanPort, connectors,
                                              quiet=True)

    def createClient(self, host, port):
        return None

    def createLogger(self, quiet):
        return MockCnCLogger(self.fullname, appender=self.__appender,
                             quiet=quiet)


class TestDAQClient(unittest.TestCase):
    def testInit(self):
        appender = MockLogger('test')
        MostlyDAQClient('foo', 0, 'localhost', 543, 0, [], appender)


if __name__ == '__main__':
    unittest.main()
