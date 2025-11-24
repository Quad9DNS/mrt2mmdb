#!/usr/bin/env python
"""
This module convert a mrt to mmdb format. This conversion also enrich the
new mmdb file with network description whereby a more rich and complete
information can be obtained from a routing prefix.
"""
import itertools
import time
import logging
from functools import wraps
from netaddr import IPSet, IPNetwork
from mmdb_writer import MMDBWriter
from tqdm import tqdm
import maxminddb
import mrtparse
from args import (
    get_args,
    mrt_arg,
    mmdb_arg,
    prefix_arg,
    target_arg,
    lookup_file_arg,
    custom_lookup_only_arg,
    quiet_arg,
    bgpscan_arg,
    prometheus_arg,
    database_type_arg,
    log_level_arg,
)
from bgpscanner import parse_bgpscanner, sanitize
from prometheus import output_prometheus
from file_stats import all_files_create, arguments_filename
from flat_file import parse_flatfile

# pylint: disable=global-statement
args = {}


def timeit(func):
    """
    measure the performance of each function call. The function needs to return a counter in order
    to determine the prefix/second value. This statistics would then be returned to the caller.
    """

    @wraps(func)
    def timeit_wrapper(*listargs, **kwargs):
        """
        decorate the calling function by adding a start stop timer. Obtain the counter and return
        all these stats.
        """
        start_time = time.perf_counter()
        result, count = func(*listargs, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        stats = (count, total_time)
        return result, stats

    return timeit_wrapper


@timeit
def make_asn_custom(fname, logger, quiet=False):
    """Make custom ASN lookup table"""
    return parse_flatfile(fname, logger, quiet)


@timeit
def make_asn(fname, logger, quiet=False):
    """
    Input:  A complete mmdb file that contains prefixes with ASN and description
    Output: Return a ASN lookup dictionary that provides a description for each ASN
            The ASN dictionary discard the prefix information. This dictionary
            is a direct ASN->Description relationship.
            Print the progress while processing each prefixes
    Workflow: Iterate over the mmdb entries to create the desire dictionary
    """
    asn = {}
    count = 0
    # Make Maxmind ASN lookup table
    message = "Making ASN table for description lookup " + fname
    if fname == "":
        logger.warning(f" {message: <80}  : skipped")
        return asn, count
    with maxminddb.open_database(fname, 1) as mreader:
        with tqdm(
            desc=f" {message: <80}  ",
            unit=" prefixes",
            disable=quiet,
        ) as pb:
            for prefix, data in mreader:
                try:
                    del prefix
                    asn[str(data["autonomous_system_number"])] = data[
                        "autonomous_system_organization"
                    ]
                    pb.update(1)
                    count += 1
                except KeyError:
                    pass
    return asn, count


@timeit
def make_routing(fname, quiet=False):
    """
    Input:  A complete mmdb file that contains prefixes with ASN and description
    Output: Return a prefix lookup dictionary with ASN as it's value
    """
    routing = {}
    count = 0
    # Make Maxmind ASN lookup table
    message = "Making routing table dictionary with prefix-key and ASN-value"
    with maxminddb.open_database(fname, 1) as mreader:
        with tqdm(
            desc=f" {message:<40}  ",
            unit=" prefixes",
            disable=quiet,
        ) as pb:
            for prefix, data in mreader:
                try:
                    routing[str(prefix)] = str(data["autonomous_system_number"])
                    pb.update(1)
                    count += 1
                except KeyError:
                    pass
    return routing, count


def make_dict(i, result):
    """
    Input: One mrt entry and a aggregated entries (result). This aggregated entries
           (dictionary) allow quick lookup of a prefix (key) and fetch the values
           (AS_PATH and the prefix).
    Output: Aggregated mrt entries in dictionary (prefix-> AS_PATH/PREFIX)
    Workflow: Check the mmrt entry for "rib_entries" as this branch contains the
              required routing information such as AS_PATH. This information are
              used to forma mrt entry in dictionary then return back to the caller.
    """
    if ("rib_entries" in i.data) and (
        len(i.data["rib_entries"][0]["path_attributes"][1]["value"]) > 0
    ):
        prefix = str(i.data["prefix"]) + "/" + str(i.data["length"])
        aspath = i.data["rib_entries"][0]["path_attributes"][1]["value"][0]["value"]
        # If as-set exist, add as-set to the aspath
        try:
            aspath = (
                aspath
                + i.data["rib_entries"][0]["path_attributes"][1]["value"][1]["value"]
            )
        except IndexError:
            pass
        result[prefix] = [aspath, prefix]
    return result


def parse_mrtparse(fname, pb, result, num_prefix):
    """Parseing of the mrtf file using mrtparse module"""
    count = 0
    mrt = mrtparse.Reader(fname)
    for i in itertools.islice(mrt, num_prefix):
        result = make_dict(i, result)
        pb.update(1)
        count += 1
    return result, count


@timeit
def load_mrt(fname):
    """
    Input: file of the mrt file.
    Output: Aggregated mrt entries in dictionary (prefix-> AS_PATH/PREFIX)
            Print the progress while processing each entry.
    Workflow: Iterate over the mrt entries (parsed by mrtparse module) to
              form the output dictionary
    """
    num_prefix = args.prefixes
    result = {}
    message = "Loading mrt data into dictionary using " + fname
    with tqdm(
        desc=f" {message: <80}  ",
        unit=" prefixes",
        disable=args.quiet,
    ) as pb:
        if args.bgpscan:
            return parse_bgpscanner(fname, pb, result, num_prefix)
        return parse_mrtparse(fname, pb, result, num_prefix)


@timeit
def convert_mrt_mmdb(fname, mrt, asn, quiet=False):
    """
    Input: Filename of the target mmdb file.
           Dictionary of the prefix->AS_PATH/PREFIX derive from previous mrt file
           Dictionary of the ASN->Decsription
    Output: Create a mmdb file on the target path
            Report any missing description as some ASN inside the mrt may not exist
            in the ASN->Decsription dictionary. This must be reported as missing
            entries.
            Print the progress of the process.
    Workflow: Iterate over the dictionary (prefix->AS_PATH/PREFIX) and derive the
              ASN of destination using AS_PATH. Using this ASN of destionation, do
              a lookup via the Dictionary of the ASN->Decsription. With all these
              data we can form a mmdb entry and using writer.insert_network to
              populate the mmdb. After the completion of the iteration, write
              all mmdb entries into the target file.
    """
    missing = []
    writer = MMDBWriter(
        ip_version=6, ipv4_compatible=True, database_type=args.database_type
    )
    count = 0
    message = "Converting mrt into mmda " + fname
    with tqdm(
        desc=f" {message: <80}  ",
        unit=" prefixes",
        disable=quiet,
    ) as pb:
        for prefix in sorted(mrt.keys(), key=lambda x: IPNetwork(x).size, reverse=True):
            try:
                val = mrt[prefix]
                as_num = sanitize(str(val[0][-1]))
                if as_num in asn:
                    org_desc = asn[as_num]
                else:
                    missing.append(as_num)
                    org_desc = "UNKNOWN"
            except IndexError:
                pass
            try:
                writer.insert_network(
                    IPSet(IPNetwork(prefix)),
                    {
                        "autonomous_system_number": int(as_num),
                        "autonomous_system_organization": org_desc,
                        "prefix": str(prefix),
                        "path": " ".join(val[0]),
                    },
                )
                pb.update(1)
                count += 1
            except (ValueError, IndexError) as e:
                # Skip problematic prefixes (typically /32 or /128 host routes)
                pb.update(1)
                continue
    message = "Writing mmda file " + fname
    with tqdm(
        desc=f" {message: <80}  ",
        unit="",
        disable=args.quiet,
    ) as pb:
        writer.to_db_file(fname)
        pb.update(1)
    return missing, count


def display_stats(text, stats, logger, quiet=False):
    """Display length of a list"""
    message = text
    if not quiet:
        logger.warning(f" {message:<80}  : {len(stats)} prefixes")
        logger.debug(f" {stats} ")


def main():
    """
    main function define the workflow to make a ASN dict->Load the
    corresponding mrt->convert the mrt into mmda
    """
    # Init route to get arguments and prase. Logging is also configured
    parser = get_args(
        [
            mrt_arg,
            mmdb_arg,
            prefix_arg,
            target_arg,
            lookup_file_arg,
            custom_lookup_only_arg,
            quiet_arg,
            bgpscan_arg,
            prometheus_arg,
            database_type_arg,
            log_level_arg,
        ]
    )
    global args
    args = parser.parse_args()

    # set up basic logging
    logging_level = getattr(logging, (args.log_level).upper(), None)
    logging.basicConfig(
        level=logging_level,
        format="",
        force=True,
    )
    logger = logging.getLogger(__name__)

    args = arguments_filename(parser, logger)

    if args.prometheus:
        # Force quiet mode in order to generate the prometheus output
        args.quiet = True
    if args.quiet:
        logging.disable(logging.WARNING)
    logger.debug(args)

    asn, asn_stats = make_asn(args.mmdb, logger, args.quiet)
    asn_custom, asn_custom_stats = make_asn_custom(args.lookup_file, logger, args.quiet)
    if args.custom_lookup_only:
        asn = asn_custom
        asn_stats = asn_custom_stats
    else:
        # merge asn lookup table for combination lookup
        asn.update(asn_custom)
    prefixes_mrt, prefix_stats = load_mrt(args.mrt)
    missing, convert_stats = convert_mrt_mmdb(
        args.target, prefixes_mrt, asn, args.quiet
    )
    display_stats("Prefixes without description", missing, logger, args.quiet)
    display_stats("ASN without description", set(missing), logger, args.quiet)
    files_stats = all_files_create(
        [args.mmdb, args.mrt, args.target, args.lookup_file], logger
    )

    if args.prometheus:
        # print prometheus formatted output by silencing all (disable logging WARNING)
        # and log the prometheus output as critical event
        logger.critical(
            output_prometheus(
                asn_stats, prefix_stats, convert_stats, missing, files_stats
            )
        )
    return 0


if __name__ == "__main__":
    main()
