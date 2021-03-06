#!/usr/bin/env python

import os
import unittest

from DAQConfig import DAQConfigParser


class CommonCode(unittest.TestCase):
    TSTRSRC = None
    OLDFMT = None
    NEWFMT = None

    @classmethod
    def __check_subdir(cls, topdir, subname):
        subdir = os.path.join(topdir, subname)
        if not os.path.exists(subdir):
            cls.fail('No "%s" subdirectory for "%s"' % (subdir, topdir))

        return subdir

    @classmethod
    def config_dir(cls, new_format=None):
        if cls.TSTRSRC is None:
            cur_dir = os.getcwd()
            tst_rsrc = os.path.join(cur_dir, 'src', 'test', 'resources',
                                    'config')
            if not os.path.exists(tst_rsrc):
                cls.fail('Cannot find test resources')
            os.environ["PDAQ_HOME"] = tst_rsrc
            cls.TSTRSRC = tst_rsrc

        # if 'new_format' is None, return the base config directory
        if new_format is None:
            return cls.TSTRSRC

        # look for the appropriate subdirectory
        if new_format:
            if cls.NEWFMT is None:
                cls.NEWFMT = cls.__check_subdir(cls.TSTRSRC, 'new_format')
            subdir = cls.NEWFMT
        else:
            if cls.OLDFMT is None:
                cls.OLDFMT = cls.__check_subdir(cls.TSTRSRC, 'old_format')
            subdir = cls.OLDFMT

        # return the subdirectory
        return subdir

    def lookup(self, cfg, data_list):
        for data in data_list:
            self.assertTrue(cfg.has_dom(data[0]),
                            "Didn't find mbid " + data[0])

        for data in data_list:
            try:
                dom = cfg.get_id_by_name(data[1])
            except ValueError:
                self.fail("Didn't find name " + data[1])
            self.assertEqual(dom, data[0],
                             'For name %s, expected %s, not %s' %
                             (data[1], data[0], dom))

        for data in data_list:
            try:
                dom = cfg.get_id_by_string_pos(data[2], data[3])
            except ValueError:
                self.fail("Didn't find string %d pos %d" % (data[2], data[3]))
            self.assertEqual(dom, data[0],
                             'For string %d pos %d, expected %s, not %s' %
                             (data[2], data[3], data[0], dom))

    def run_names_test(self, new_format):
        cfg_dir = self.config_dir(new_format=new_format)

        simpledata = ("simpleConfig", 5, "IniceGlobalTest")
        sps40data = ("sps-IC40-IT6-AM-Revert-IceTop-V029", 41,
                     "sps-icecube-amanda-008")
        for data in (simpledata, sps40data):
            cfgname = data[0]

            cfg = DAQConfigParser.parse(cfg_dir, cfgname)
            self.assertEqual(cfgname, cfg.basename,
                             "Expected %s, not %s" % (cfgname, cfg.basename))
            fullname = os.path.join(cfg_dir, cfgname + ".xml")
            self.assertEqual(fullname, cfg.fullpath,
                             "Expected %s, not %s" %
                             (fullname, cfg.fullpath))

            domnames = cfg.dom_configs
            self.assertEqual(data[1], len(domnames),
                             "Expected %s dom names in %s, not %s" %
                             (data[1], cfgname, len(domnames)))

            trigcfg = cfg.trigger_config
            self.assertEqual(data[2], trigcfg.basename,
                             "Expected trigger config %s in %s, not %s" %
                             (data[2], cfgname, trigcfg.basename))

    def run_lists_sim5_test(self, new_format):
        cfg_dir = self.config_dir(new_format=new_format)
        cfg = DAQConfigParser.parse(cfg_dir, "simpleConfig")

        expected = ['eventBuilder', 'globalTrigger', 'inIceTrigger',
                    'secondaryBuilders', 'stringHub#1001', 'stringHub#1002',
                    'stringHub#1003', 'stringHub#1004', 'stringHub#1005']

        comps = cfg.components

        self.assertEqual(len(expected), len(comps),
                         "Expected %d components (%s), not %d (%s)" %
                         (len(expected), str(expected), len(comps),
                          str(comps)))

        for comp in comps:
            try:
                expected.index(comp.fullname)
            except ValueError:
                self.fail('Unexpected component "%s"' % comp)

    def run_lookup_sim5_test(self, new_format):
        cfg_dir = self.config_dir(new_format=new_format)
        cfg = DAQConfigParser.parse(cfg_dir, "simpleConfig")

        data_list = (
            ('53494d550101', 'Nicholson_Baker', 1001, 1),
            ('53494d550120', 'SIM0020', 1001, 20),
            ('53494d550140', 'SIM0040', 1001, 40),
            ('53494d550160', 'SIM0060', 1001, 60),
            ('53494d550201', 'SIM0065', 1002, 1),
            ('53494d550220', 'SIM0084', 1002, 20),
            ('53494d550240', 'SIM0104', 1002, 40),
            ('53494d550260', 'SIM0124', 1002, 60),
            ('53494d550301', 'SIM0129', 1003, 1),
            ('53494d550320', 'SIM0148', 1003, 20),
            ('53494d550340', 'SIM0168', 1003, 40),
            ('53494d550360', 'SIM0188', 1003, 60),
            ('53494d550401', 'SIM0193', 1004, 1),
            ('53494d550420', 'SIM0212', 1004, 20),
            ('53494d550440', 'SIM0232', 1004, 40),
            ('53494d550460', 'SIM0252', 1004, 60),
            ('53494d550501', 'SIM0257', 1005, 1),
            ('53494d550520', 'SIM0276', 1005, 20),
            ('53494d550540', 'SIM0296', 1005, 40),
            ('53494d550560', 'SIM0316', 1005, 60),
            )

        self.lookup(cfg, data_list)

    def run_lists_sps_ic40_it6_test(self, new_format):
        cfg_dir = self.config_dir(new_format=new_format)
        cfg = DAQConfigParser.parse(cfg_dir,
                                    "sps-IC40-IT6-AM-Revert-IceTop-V029")

        expected = ['amandaTrigger', 'eventBuilder', 'globalTrigger',
                    'iceTopTrigger', 'inIceTrigger', 'secondaryBuilders',
                    'stringHub#0', 'stringHub#21', 'stringHub#29',
                    'stringHub#30', 'stringHub#38', 'stringHub#39',
                    'stringHub#40', 'stringHub#44', 'stringHub#45',
                    'stringHub#46', 'stringHub#47', 'stringHub#48',
                    'stringHub#49', 'stringHub#50', 'stringHub#52',
                    'stringHub#53', 'stringHub#54', 'stringHub#55',
                    'stringHub#56', 'stringHub#57', 'stringHub#58',
                    'stringHub#59', 'stringHub#60', 'stringHub#61',
                    'stringHub#62', 'stringHub#63', 'stringHub#64',
                    'stringHub#65', 'stringHub#66', 'stringHub#67',
                    'stringHub#68', 'stringHub#69', 'stringHub#70',
                    'stringHub#71', 'stringHub#72', 'stringHub#73',
                    'stringHub#74', 'stringHub#75', 'stringHub#76',
                    'stringHub#77', 'stringHub#78']

        comps = cfg.components

        self.assertEqual(len(expected), len(comps),
                         "Expected %d components (%s), not %d (%s)" %
                         (len(expected), str(expected), len(comps),
                          str(comps)))

        for comp in comps:
            try:
                expected.index(comp.fullname)
            except ValueError:
                self.fail('Unexpected component "%s"' % comp)

    def run_lookup_sps_ic40_it6_test(self, new_format):
        cfg_dir = self.config_dir(new_format=new_format)
        cfg = DAQConfigParser.parse(cfg_dir,
                                    "sps-IC40-IT6-AM-Revert-IceTop-V029")

        data_list = (
            ('737d355af587', 'Bat', 21, 1),
            ('499ccc773077', 'Werewolf', 66, 6),
            ('efc9607742b9', 'Big_Two_Card', 78, 60),
            ('1e5b72775d19', 'AMANDA_SYNC_DOM', 0, 91),
            ('1d165fc478ca', 'AMANDA_TRIG_DOM', 0, 92),
        )

        self.lookup(cfg, data_list)

    def run_dump_doms_test(self, new_format):
        cfg_dir = self.config_dir(new_format=new_format)
        cfg = DAQConfigParser.parse(cfg_dir,
                                    "sps-IC40-IT6-AM-Revert-IceTop-V029")

        for dom in cfg.all_doms:
            mbid = str(dom)
            if len(mbid) != 12 or mbid.startswith(" "):
                self.fail("DOM %s(%s) has bad MBID" % (mbid, dom.name))
            num = 0
            if str(dom).startswith("0"):
                num += 1
                nmid = cfg.get_id_by_name(dom.name)
                if nmid != mbid:
                    self.fail("Bad IDbyName value \"%s\" for \"%s\"" %
                              (nmid, mbid))

                newid = cfg.get_id_by_string_pos(dom.string, dom.pos)
                if newid.startswith(" ") or len(newid) != 12:
                    self.fail("Bad IDbyStringPos value \"%s\" for \"%s\" %d" %
                              (newid, mbid, num))

    def run_replay_test(self, new_format):
        cfg_dir = self.config_dir(new_format=new_format)
        cfg = DAQConfigParser.parse(cfg_dir, "replay-ic22-it4")

        expected = ['eventBuilder', 'globalTrigger', 'iceTopTrigger',
                    'inIceTrigger',
                    'replayHub#21', 'replayHub#29', 'replayHub#30',
                    'replayHub#38', 'replayHub#39', 'replayHub#40',
                    'replayHub#46', 'replayHub#47', 'replayHub#48',
                    'replayHub#49', 'replayHub#50', 'replayHub#56',
                    'replayHub#57', 'replayHub#58', 'replayHub#59',
                    'replayHub#65', 'replayHub#66', 'replayHub#67',
                    'replayHub#72', 'replayHub#73', 'replayHub#74',
                    'replayHub#78', 'replayHub#201', 'replayHub#202',
                    'replayHub#203', 'replayHub#204']

        comps = cfg.components

        self.assertEqual(len(expected), len(comps),
                         "Expected %d components (%s), not %d (%s)" %
                         (len(expected), str(expected), len(comps),
                          str(comps)))

        for comp in comps:
            try:
                expected.index(comp.fullname)
            except ValueError:
                self.fail('Unexpected component "%s"' % comp)


class DAQNewConfigTest(CommonCode):
    def test_names(self):
        self.run_names_test(True)

    def test_lists_sim5(self):
        self.run_lists_sim5_test(True)

    def test_lookup_sim5(self):
        self.run_lookup_sim5_test(True)

    def test_lists_sps_ic40_it6(self):
        self.run_lists_sps_ic40_it6_test(True)

    def test_lookup_sps_ic40_it6(self):
        self.run_lookup_sps_ic40_it6_test(True)

    def test_dump_doms(self):
        self.run_dump_doms_test(True)

    def test_replay(self):
        self.run_replay_test(True)


class DAQOldConfigTest(CommonCode):
    def test_names(self):
        self.run_names_test(False)

    def test_lists_sim5(self):
        self.run_lists_sim5_test(False)

    def test_lookup_sim5(self):
        self.run_lookup_sim5_test(False)

    def test_lists_sps_ic40_it6(self):
        self.run_lists_sps_ic40_it6_test(False)

    def test_lookup_sps_ic40_it6(self):
        self.run_lookup_sps_ic40_it6_test(False)

    def test_dump_doms(self):
        self.run_dump_doms_test(False)

    def test_replay(self):
        self.run_replay_test(False)


class DAQConfigTest(CommonCode):
    def test_check_period(self):
        cfg_dir = self.config_dir()

        cfg = DAQConfigParser.parse(cfg_dir, "sps-IC40-hitspool")

        exp_val = 10
        self.assertEqual(exp_val, cfg.monitor_period,
                         "Expected monitor period for %s to be %d, not %s" %
                         (cfg.basename, exp_val, cfg.monitor_period))

        exp_val = 25
        self.assertEqual(exp_val, cfg.watchdog_period,
                         "Expected watchdog period for %s to be %d, not %s" %
                         (cfg.basename, exp_val, cfg.watchdog_period))


if __name__ == '__main__':
    unittest.main()
