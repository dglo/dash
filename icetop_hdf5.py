#!/usr/bin/env python

import datetime
import logging
import os
import re
import tarfile

from lxml import etree

import h5py
import numpy
import payload

from DefaultDomGeometry import DefaultDomGeometryReader


MONI_PAT = re.compile(r"^moni_(\d+)_(\d+)_\d+_\d+\.dat$")

H5_TYPES = [
    ('DOM_String', '<u2'),
    ('DOM_Position', '<u2'),
    ('SPE', '<u4'),
    ('MPE', '<u4'),
    ('HITS', 'u4'),
    ('DEADTIME', '<u4'),
    ('UT', '<u8'),
]


def convert_pairs_to_xml(root, pairs):
    """
    Add a list/tuple of lists/tuples of pairs of data to an XML tree.
    The 'pairs' list is a dumber version of an ordered dictionary
    """
    for key, val in pairs:
        node = etree.SubElement(root, key)
        if isinstance(val, tuple) or isinstance(val, list):
            convert_pairs_to_xml(node, val)
        else:
            node.text = str(val)


def create_meta_xml(path, suffix, run_number, verbose=False, dry_run=False):
    """
    Create a metadata file for JADE.  Metadata specification is at:
    https://docushare.icecube.wisc.edu/dsweb/Get/Document-20546/metadata_specification.pdf
    """

    # give up if they're passing us a bad filename/suffix pair
    if not path.endswith(suffix):
        raise SystemExit("File \"%s\" does not end with \"%s\"" %
                         (path, suffix))

    # define all the static fields
    title = "Icetop Scaler Data"
    summary = title
    category = "monitoring"
    subcategory = "IceTopScaler"

    # we'll need the file creation date for a few fields
    if dry_run:
        filetime = datetime.datetime.now()
    else:
        try:
            stamp = os.path.getmtime(path)
        except OSError:
            raise SystemExit("Cannot write metadata file: %s does not exist" %
                             (path, ))
        filetime = datetime.datetime.fromtimestamp(stamp)

    # we'll need the directory and base filename below
    directory = os.path.dirname(path)
    basename = os.path.basename(path)[:-len(suffix)]
    if basename[-1] == ".":
        basename = basename[:-1]

    # fill in all the file-related fields
    timestr = filetime.strftime("%Y-%m-%dT%H:%M:%S")
    plus_fields = (
        ("Start_DateTime", timestr),
        ("End_DateTime", timestr),
        ("Category", category),
        ("Subcategory", subcategory),
        ("Run_Number", run_number),
    )

    xmldict = (
        ("DIF", (
            ("Entry_ID", basename),
            ("Entry_Title", title),
            ("Parameters",
             "SPACE SCIENCE > Astrophysics > Neutrinos"),
            ("ISO_Topic_Category", "geoscientificinformation"),
            ("Data_Center", (
                ("Data_Center_Name",
                 "UWI-MAD/A3RI > Antarctic Astronomy and Astrophysics"
                 " Research Institute, University of Wisconsin, Madison"),
                ("Personnel", (
                    ("Role", "Data Center Contact"),
                    ("Email", "datacenter@icecube.wisc.edu"),
                )),
            )),
            ("Summary", summary),
            ("Metadata_Name", "[CEOS IDN DIF]"),
            ("Metadata_Version", "9.4"),
            ("Personnel", (
                ("Role", "Technical Contact"),
                ("First_Name", "Dave"),
                ("Last_Name", "Glowacki"),
                ("Email", "dglo@icecube.wisc.edu"),
            )),
            ("Sensor_Name", "ICECUBE > IceCube"),
            ("Source_Name",
             "EXPERIMENTAL > Data with an instrumentation based"
             " source"),
            ("DIF_Creation_Date", filetime.strftime("%Y-%m-%d")),
        )),
        ("Plus", plus_fields),
    )

    root = etree.Element("DIF_Plus")
    root.set("{http://www.w3.org/2001/XMLSchema-instance}"
             "noNamespaceSchemaLocation", "IceCubeDIFPlus.xsd")

    convert_pairs_to_xml(root, xmldict)

    metaname = basename + ".meta.xml"
    if verbose:
        print "Creating JADE semaphore file %s" % (metaname, )
    if not dry_run:
        with open(os.path.join(directory, metaname), "w") as out:
            out.write(etree.tostring(root))

    return metaname


def process_moni(dom_dict, moniname):
    """
    Return a list of all IceTop "fast" monitoring records in this file
    """
    data = []
    with payload.PayloadReader(moniname) as rdr:
        for pay in rdr:
            if not isinstance(pay, payload.MonitorRecord):
                logging.error("Ignoring %s payload %s", moniname, pay)
                continue

            if pay.subtype != payload.MonitorASCII.SUBTYPE_ID:
                # only want ASCII records
                continue

            key = "%012x" % pay.dom_id
            if key not in dom_dict:
                logging.error("Ignoring unknown DOM %012x", key)
                continue

            dom = dom_dict[key]
            if not dom.is_icetop:
                continue

            if not pay.text.startswith("F "):
                # only want FAST records
                continue

            flds = []
            for tmp in pay.text[2:].split():
                try:
                    flds.append(int(tmp))
                except ValueError:
                    logging.error("Bad FAST data \"%s\"", pay.text)
                    continue

            if len(flds) != 4:
                logging.error("Too many fields in FAST data \"%s\"", pay.text)
                continue

            (spe_count, mpe_count, launches, deadtime) = flds
            data.append((dom.originalString(), dom.pos(), spe_count, mpe_count,
                         launches, deadtime, pay.utime))

    return data


def process_list(monilist, dom_dict, verbose=False, dry_run=False,
                 debug=False):
    "Process all .moni files in the list"
    run = None
    data = []

    for moni in monilist:
        match = MONI_PAT.match(moni)
        if match is None:
            if debug:
                print "BADNAME %s" % (moni, )
            continue

        # get the run number from the file name
        frun = int(match.group(1))
        fseq = int(match.group(2))

        # if we've got data from another run, write it to a file
        if run is not None and run != frun:
            write_data(run, data, verbose=verbose, dry_run=dry_run,
                       make_meta_xml=True)
            del data[:]

        # save the new data
        run = frun
        if verbose:
            print "Processing run %d moni file #%d" % (frun, fseq)
        data += process_moni(dom_dict, moni)

    if run is not None:
        write_data(run, data, verbose=verbose, dry_run=dry_run,
                   make_meta_xml=True)


def process_tar_file(tarname, dom_dict, verbose=False, debug=True):
    "Process all .moni files in the tar file"
    run = None
    data = []

    tfl = tarfile.open(tarname, "r")
    for info in tfl.getmembers():
        if not info.isfile():
            if debug:
                print "NONFILE[%s] %s" % (tarname, info.name, )
            continue

        if info.name.find("moni_") < 0:
            if debug:
                print "NONMONI[%s] %s" % (tarname, info.name)
            continue

        match = MONI_PAT.match(info.name)
        if match is None:
            if debug:
                print "BADNAME[%s] %s" % (tarname, info.name)
            continue

        # get the run number from the file name
        frun = int(match.group(1))

        # if we've got data from another run, write it to a file
        if run is not None and run != frun:
            write_data(run, data, verbose=verbose)
            del data[:]

        # extract this file from the tarfile
        tfl.extract(info)

        try:
            # save the new data
            run = frun
            data += process_moni(dom_dict, info.name)
        finally:
            os.unlink(info.name)

    if run is not None:
        write_data(run, data, verbose=verbose)


def write_data(run, data, verbose=False, dry_run=False, make_meta_xml=False):
    "Write IceTop monitoring data to an HDF5 file"

    # ignore empty arrays
    if data is None or len(data) == 0:
        return

    # define file suffix here since we'll need it in a couple of places
    suffix = ".hdf5"

    # assemble the base name for the file
    now = datetime.datetime.now()
    basename = "IceTop_%06d_%04d%02d%02d_%02d%02d%02d" % \
               (run, now.year, now.month, now.day, now.hour, now.minute,
                now.second)

    # find the next unused filename
    seq = 0
    while True:
        filename = "%s_%d%s" % (basename, seq, suffix)
        if not os.path.exists(filename):
            break
        seq += 1

    if verbose:
        print "Writing %s" % (filename, )

    # write data
    if not dry_run:
        with h5py.File(filename, "w") as out:
            # create a NumPy array from the data
            narray = numpy.array(data, dtype=H5_TYPES)
            # add the data to the file
            out.create_dataset("FastIceTop", data=narray, chunks=True)

    if make_meta_xml:
        create_meta_xml(filename, suffix, run, verbose=verbose,
                        dry_run=dry_run)


if __name__ == "__main__":
    # pylint: disable=invalid-name,wrong-import-position
    import sys

    verbose = False
    files = []
    for fname in sys.argv[1:]:
        if fname == "-v":
            verbose = True
        elif os.path.exists(fname):
            files.append(fname)
        else:
            logging.error("Ignoring unknown file \"%s\"", fname)

    # read in default-dom-geometry.xml
    ddg = DefaultDomGeometryReader.parse(translateDoms=True)

    # cache the DOM ID -> DOM dictionary
    ddict = ddg.getDomIdToDomDict()

    for fname in files:
        process_tar_file(fname, ddict, verbose=verbose)
