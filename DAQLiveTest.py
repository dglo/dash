#!/usr/bin/env python

import sys, thread, unittest
from DAQConst import DAQPort
from DAQRPC import RPCServer

TEST_LIVE = True
try:
    from DAQLive import DAQLive, LiveArgs
except SystemExit:
    TEST_LIVE = False
    class DAQLive:
        pass

from DAQMocks import SocketReaderFactory

class MockLive(DAQLive):
    def __init__(self, port):
        super(MockLive, self).__init__(self.__buildArgs(port))

    def __buildArgs(self, port, extraArgs=None):
        stdArgs = { '-v' : '',
                    '-P' : str(port) }

        oldArgv = sys.argv
        try:
            sys.argv = ['foo']

            for k in stdArgs.keys():
                if extraArgs is None or not extraArgs.has_key(k):
                    sys.argv.append(k)
                    if len(stdArgs[k]) > 0:
                        sys.argv.append(stdArgs[k])

            if extraArgs is not None:
                for k in extraArgs.keys():
                    sys.argv.append(k)
                    if len(extraArgs[k]) > 0:
                        sys.argv.append(extraArgs[k])

            args = LiveArgs()
            args.parse()
        finally:
            sys.argv = oldArgv

        return args

class MockRun(object):
    def __init__(self):
        self.__state = 'IDLE'

        self.__evtCounts = {}

        self.__rpc = RPCServer(DAQPort.DAQRUN)
        self.__rpc.register_function(self.__recover, 'rpc_recover')
        self.__rpc.register_function(self.__monitor, 'rpc_run_monitoring')
        self.__rpc.register_function(self.__getState, 'rpc_run_state')
        self.__rpc.register_function(self.__startRun, 'rpc_start_run')
        self.__rpc.register_function(self.__stopRun, 'rpc_stop_run')
        thread.start_new_thread(self.__rpc.serve_forever, ())

    def __getState(self):
        return self.__state

    def __monitor(self):
        return self.__evtCounts

    def __recover(self):
        self.__state = 'STOPPED'
        return 1

    def __startRun(self, runNum, subRunNum, cfgName, logInfo=None):
        self.__state = 'RUNNING'
        return 1

    def __stopRun(self):
        self.__state = 'STOPPED'
        return 1

    def close(self):
        self.__rpc.server_close()

    def setEventCounts(self, physics, moni, sn, tcal):
        self.__evtCounts.clear()
        self.__evtCounts["physicsEvents"] = physics
        self.__evtCounts["moniEvents"] = moni
        self.__evtCounts["snEvents"] = sn
        self.__evtCounts["tcalEvents"] = tcal

class TestDAQLive(unittest.TestCase):
    def setUp(self):
        self.__live = None
        self.__run = None
        self.__logFactory = SocketReaderFactory()

    def tearDown(self):
        self.__logFactory.tearDown()
        if self.__run is not None:
            self.__run.close()
        if self.__live is not None:
            self.__live.close()

    def testStartNoConfig(self):
        if not TEST_LIVE:
            print 'Skipping I3Live-related test'
            return

        log = self.__logFactory.createLog('liveMoni', DAQPort.I3LIVE, False)

        port = 9876

        log.addExpectedText('Connecting to DAQRun')
        log.addExpectedText('Started pdaq service on port %d' % port)

        self.__live = MockLive(port)

        self.assertRaises(Exception, self.__live.starting, {})

        log.checkStatus(10)

    def testStart(self):
        if not TEST_LIVE:
            print 'Skipping I3Live-related test'
            return

        log = self.__logFactory.createLog('liveMoni', DAQPort.I3LIVE, False)

        port = 9876

        log.addExpectedText('Connecting to DAQRun')
        log.addExpectedText('Started pdaq service on port %d' % port)

        self.__live = MockLive(port)

        self.__run = MockRun()

        runConfig = 'xxxCfg'
        runNum = 543
        
        log.addExpectedText('Starting run %d - %s'% (runNum, runConfig))
        log.addExpectedText('Started run %d'% runNum)

        args = {'runConfig':runConfig, 'runNumber':runNum}
        self.__live.starting(args)

    def testStop(self):
        if not TEST_LIVE:
            print 'Skipping I3Live-related test'
            return

        log = self.__logFactory.createLog('liveMoni', DAQPort.I3LIVE, False)

        port = 9876

        log.addExpectedText('Connecting to DAQRun')
        log.addExpectedText('Started pdaq service on port %d' % port)

        self.__live = MockLive(port)

        self.__run = MockRun()

        runNum = 0

        log.addExpectedText('Stopping run %d'% runNum)
        log.addExpectedText('Stopped run %d'% runNum)

        numPhysics = 5
        numMoni = 10
        numSn = 15
        numTcal = 20

        self.__run.setEventCounts(numPhysics, numMoni, numSn, numTcal)

        log.addExpectedLiveMoni('tcalEvents', numTcal)
        log.addExpectedLiveMoni('moniEvents', numMoni)
        log.addExpectedLiveMoni('snEvents', numSn)
        log.addExpectedLiveMoni('physicsEvents', numPhysics)

        self.__live.stopping()

    def testRecover(self):
        if not TEST_LIVE:
            print 'Skipping I3Live-related test'
            return

        log = self.__logFactory.createLog('liveMoni', DAQPort.I3LIVE, False)

        port = 9876

        log.addExpectedText('Connecting to DAQRun')
        log.addExpectedText('Started pdaq service on port %d' % port)

        self.__live = MockLive(port)

        self.__run = MockRun()

        log.addExpectedText('Recovering pDAQ')
        log.addExpectedText('Recovered DAQ')

        self.__live.recovering()

if __name__ == '__main__':
    unittest.main()