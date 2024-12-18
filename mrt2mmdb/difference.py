#!/usr/bin/env python
"""
Show the differences in different object files eg. Maxmind mmdb vs custom lookup file
"""
import sys
import logging
import json
from deepdiff import DeepDiff

from make_mmdb import (
    make_asn_custom,
    make_asn,
    make_routing,
)
from args import (
    get_args,
    compare_asn_arg,
    mmdb_arg,
    lookup_file_arg,
    compare_routing_arg,
    print_changes_arg,
    quiet_arg,
    log_level_arg,
)


# pylint: disable=global-statement
def compare(dict0, dict1, args, logger):
    """Compare two dictionaries then print out the statistics
    if --print_changes is set, output the changes in json format
    """
    if args.quiet:
        freq = 0
    else:
        freq = 1
    diff = DeepDiff(
        dict0,
        dict1,
        log_frequency_in_sec=freq,
        progress_logger=logger.warning,
    )
    for i in (delta := list(diff)):
        logger.warning(f"{i} = {len(diff[i])}")
    if args.print_changes and "values_changed" in delta:
        logger.warning(json.dumps(diff["values_changed"], indent=1))
    return diff


def main():
    """
    main function to get the respective files for processing the differences
    """
    parser = get_args(
        [
            compare_asn_arg,
            mmdb_arg,
            lookup_file_arg,
            log_level_arg,
            compare_routing_arg,
            print_changes_arg,
            quiet_arg,
        ]
    )
    args = parser.parse_args()

    # set up basic logging
    logging_level = getattr(logging, (args.log_level).upper(), None)
    logging.basicConfig(
        stream=sys.stdout,
        level=logging_level,
        format="",
        force=True,
    )
    logger = logging.getLogger(__name__)
    logger.debug(args)

    # Turn off progress report in quiet mode
    # Frequency of 0 sec is to supress progress report

    if args.compare_routing is not None and len(args.compare_routing) == 2:
        routing0, _ = make_routing(args.compare_routing[0], args.quiet)
        routing1, _ = make_routing(args.compare_routing[1], args.quiet)
        compare(routing0, routing1, args, logger)

    if args.compare_asn and args.lookup_file != "" and args.mmdb != "":
        asn0, _ = make_asn(args.mmdb, args.quiet)
        asn1, _ = make_asn_custom(args.lookup_file, args.quiet)
        compare(asn0, asn1, args, logger)
    return 0


if __name__ == "__main__":
    main()
