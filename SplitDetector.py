#!/usr/bin/env python
"""
`pdaq generate-config-set` script which generates the alternate and
"-no##" configs from a full detector run configuration
"""

# J. Kelley
# 15 January 2013

from __future__ import print_function

from DefaultDomGeometry import DefaultDomGeometryReader
from DAQConfig import DAQConfigException, DAQConfigParser
from RemoveHubs import create_config, get_hub_name
from locate_pdaq import find_pdaq_config


# list of icetop and in-ice keys used to build each alternate configuration
PARTITION_KEYS = (
    ("NORTH", "INICE"),
    ("SOUTH", "INICE"),
    ("NORTH", "IINORTH"),
    ("SOUTH", "IISOUTH"),
    ("NORTH", "NORTHEAST"),
    ("NORTH", "NORTHWEST"),
    ("SOUTH", "SOUTHEAST"),
    ("SOUTH", "SOUTHWEST"),
)


class SplitException(Exception):
    "General SplitDetector exception"


def add_arguments(parser):
    "Add command-line arguments"

    parser.add_argument("-A", "--altconfigs_only", dest="altconfigs_only",
                        action="store_true", default=False,
                        help="Only generate alternate run configurations")
    parser.add_argument("-f", "--force", dest="force",
                        action="store_true", default=False,
                        help="Overwrite existing configuration file(s)")
    parser.add_argument("-N", "--noxx_only", dest="noxx_only",
                        action="store_true", default=False,
                        help="Only generate -noxx run configurations")
    parser.add_argument("-n", "--dry-run", dest="dryrun",
                        action="store_true", default=False,
                        help="Dry run (do not actually create configurations)")
    parser.add_argument("-P", "--partitions_only", dest="altconfigs_only",
                        action="store_true", default=False,
                        help="Obsolete alias for --altconfigs_only")
    parser.add_argument("-p", "--print-testdaq", dest="print_testdaq",
                        action="store_true", default=False,
                        help="Print domhubConfig.dat sections for testdaq")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", default=False,
                        help="Verbose mode")
    parser.add_argument("run_config", nargs=1,
                        help="Run configuration file")


def __make_alternate_config(run_config, altconfigs, it_key, ii_key,
                            dry_run=False, force=False, verbose=False):
    if it_key not in ("NORTH", "SOUTH"):
        raise SplitException("Bad IceTop key \"%s\"" % it_key)

    basename = run_config.filename
    if basename.endswith(".xml"):
        basename = basename[:-4]

    cfgname = "IceTop" + it_key
    tstname = "icetop_" + it_key.lower()

    if ii_key == "INICE":
        cfgname += ""
        tstname += "_inice"
    elif ii_key.startswith("II"):
        cfgname += "_InIce" + ii_key[2:]
        tstname += "_inice_" + ii_key[2:].lower()
    else:
        cfgname += "_InIce" + ii_key
        tstname += "_inice_" + ii_key.lower()

    hub_list = sorted(altconfigs[it_key] + altconfigs[ii_key])

    if verbose:
        print("Generating alternate configuration %s" % cfgname)
        print("  using hubs %s" % range_string(hub_list))

    new_name = "%s_%s_partition.xml" % (basename, cfgname)
    if not dry_run:
        _ = create_config(run_config, hub_list, None, new_name=new_name,
                          keep_hubs=True, force=force, verbose=verbose)
    elif verbose:
        print("  writing to %s" % (new_name, ))
    else:
        print("%s: %s" % (new_name, range_string(hub_list)))

    return tstname, hub_list


def get_altconfigs(verbose=False):
    """
    Get lists of hubs associated with each alternate configuration.
    Partitions "NORTH" and "SOUTH" should include all IceTop hubs.
    Partitions "NORTHEAST", "NORTHWEST", "SOUTHEAST", and "SOUTHWEST" should
    include all In-Ice hubs.
    Partitions "IINORTH" and "IISOUTH" are supersets of the In-Ice "NORTH*"
    and "SOUTH*" partitions.
    Partition "INICE" is a superset of all In-Ice hubs.
    """

    if verbose:
        print("Reading DOM geometry data")

    # read in default-dom-geometry.xml
    def_dom_geom = DefaultDomGeometryReader.parse()

    # get partition definitions
    altconfigs = def_dom_geom.partitions

    if verbose:
        print("Sanity-checking all configurations")

    # build tuples will all IceTop and In-Ice alternate configuration keys
    icetop_keys = ("NORTH", "SOUTH")
    inice_keys = ("NORTHEAST", "NORTHWEST", "SOUTHEAST", "SOUTHWEST")

    # make sure alternate configurations include all strings and stations
    all_icetop = [x for x in range(201, 212)]
    sanity_check(altconfigs, "IceTop", icetop_keys, all_icetop)
    all_inice = [x for x in range(1, 87)]
    sanity_check(altconfigs, "InIce", inice_keys, all_inice)

    # fill in superset alternate configurations
    altconfigs["IINORTH"] = altconfigs["NORTHEAST"] + altconfigs["NORTHWEST"]
    altconfigs["IISOUTH"] = altconfigs["SOUTHEAST"] + altconfigs["SOUTHWEST"]
    altconfigs["INICE"] = altconfigs["IINORTH"] + altconfigs["IISOUTH"]

    return altconfigs


def range_string(hub_list):
    """
    Create a concise string from a list of numbers
    (e.g. [1, 3, 4, 5, 7, 8, 9] will return "1,3-5,7-9")
    """
    prev_hub = None
    range_str = ""
    for hub in sorted(hub_list):
        if prev_hub is None:
            range_str += "%d" % hub
        elif hub == prev_hub + 1:
            if not range_str.endswith("-"):
                range_str += "-"
        else:
            if range_str.endswith("-"):
                range_str += "%d" % prev_hub
            range_str += ",%d" % hub
        prev_hub = hub
    if prev_hub is not None and range_str.endswith("-"):
        range_str += "%d" % prev_hub
    return range_str


def sanity_check(altconfigs, name, keys, expected):
    """
    If the hubs from the alternate configurations listed in "keys" do not
    match the hubs in "expected", complain and exit
    """

    # first make sure the individual alternate configurations don't overlap
    for key1 in keys:
        for key2 in keys:
            if key1 == key2:
                continue
            overlap = [hub for hub in altconfigs[key1]
                       if hub in altconfigs[key2]]
            if len(overlap) > 0:  # pylint: disable=len-as-condition
                raise SystemExit("Alternate configurations \"%s\" and \"%s\""
                                 " both contain hubs %s" %
                                 (key1, key2, overlap))

    # build a list containing all the hubs from each alternate configuration
    testlist = []
    for part in keys:
        if part not in altconfigs:
            raise SystemExit("No %s %s alternate configuration found" %
                             (name, part))

        testlist += altconfigs[part]
    testlist.sort()

    # die if the final list doesn't contain all the expected hubs
    if testlist != expected:
        print("=== EXPECTED\n%s" % str(expected))
        print("=== RECEIVED\n%s" % str(testlist))
        raise SystemExit("Bad %s alternate configurations (missing or"
                         " duplicate hubs)!" % (name, ))


def split_detector(args):
    gen_altconfigs = True
    gen_noxx = True
    if args.altconfigs_only or args.noxx_only:
        if not args.altconfigs_only:
            gen_altconfigs = False
        if not args.noxx_only:
            gen_noxx = False

    if args.verbose:
        print("Finding pDAQ configuration directory")

    # find the pDAQ configuration directory
    config_dir = find_pdaq_config()

    if args.verbose:
        print("Reading run configuration \"%s\"" % args.run_config[0])

    try:
        run_config = DAQConfigParser.parse(config_dir, args.run_config[0])
    except DAQConfigException as config_except:
        raise SystemExit(str(args.run_config) + ": " + str(config_except))

    # map generated configuration names to lists of included hubs
    tstlist = {}

    if gen_altconfigs:
        if args.verbose:
            print("Generating alternate versions of %s" %
                  (run_config.basename, ))

        # get alternate configuration descriptions
        altconfigs = get_altconfigs(verbose=args.verbose)

        # build alternate configurations
        for it_key, ii_key in PARTITION_KEYS:
            (tstname, hubs) = __make_alternate_config(run_config, altconfigs,
                                                      it_key, ii_key,
                                                      dry_run=args.dryrun,
                                                      force=args.force,
                                                      verbose=args.verbose)
            if args.print_testdaq:
                tstlist[tstname] = hubs

    if gen_noxx:
        if args.verbose:
            print("Generating noxx versions of %s" % (run_config.basename, ))
        for comp in run_config.components:
            if comp.is_hub:
                if not args.dryrun:
                    _ = create_config(run_config, [comp.id, ], None,
                                      force=args.force, verbose=args.verbose)
                elif args.verbose:
                    print("  writing to %s-no%s" %
                          (run_config.basename, get_hub_name(comp.id)))
                else:
                    print("%s-no%s" %
                          (run_config.basename, get_hub_name(comp.id)))

                # XXX not adding noxx config to tstlist

    if args.print_testdaq:
        print("domhubConfig.dat sections:")

        first = True
        for tstname, hubs in list(tstlist.items()):
            if first:
                first = False
            else:
                print("")

            print("\"%s\"" % tstname)

            hubs.sort()
            for hub in hubs:
                if hub < 200:
                    print("sps-ichub%02d" % hub)
                elif hub < 212:
                    print("sps-ithub%02d" % (hub - 200))
                else:
                    print("unknown%02d" % hub)


def main():
    "Main program"

    import argparse

    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    split_detector(args)


if __name__ == "__main__":
    main()
