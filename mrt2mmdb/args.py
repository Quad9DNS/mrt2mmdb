#!/usr/bin/env python
"""
This module get the arguments for the various founction for processing
Any new argument can be added to this file to consolidate and reuse all possible
arguments used. Use need to add the boolean keyword of the new argument and use the
boolean variable to capture the argument needed when this module is called.
"""
import argparse


def mrt_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--mrt",
        metavar="",
        type=str,
        help="Filename of mrt dump",
        default="data/mrt-dump.ams.202402171710.gz",
    )


def mmdb_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--mmdb",
        metavar="",
        type=str,
        help="Filename of Maxmind mmdb file for prefixes lookup and return description/ASN",
        default="data/GeoLite2-ASN.mmdb",
    )


def geonames_cities_arg(parser):
    """geonames filename, containing cities populations"""
    return parser.add_argument(
        "--geonames_cities",
        metavar="",
        type=str,
        help="Filename of geonames cities file for cities population lookup"
        "(e.g. txt file from "
        "https://download.geonames.org/export/dump/cities15000.zip)",
        default="",
    )


def admincodes_arg(parser):
    """geonames admincodes, containing administrative division names
    (e.g. US states)"""
    return parser.add_argument(
        "--admincodes",
        metavar="",
        type=str,
        help="Filename of geonames adminc1codes file "
        "(e.g. "
        "https://download.geonames.org/export/dump/admin1CodesASCII.txt)",
        default="",
    )


def min_population_arg(parser):
    """minimal population count allowed in dataset cities"""
    return parser.add_argument(
        "--min_population",
        metavar="",
        type=int,
        help="Minimal population count allowed in dataset cities"
        "(default: 15000)",
        default=15000,
    )


def target_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--target",
        metavar="",
        type=str,
        help="Filename of new target mmdb file generated from mmrt file",
        default="out.mmdb",
    )


def lookup_file_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--lookup_file",
        metavar="",
        type=str,
        help="Filename of csv,tsv file for custom ASN lookup",
        default="",
        #            default="data/asn_rir_org_country.csv",
    )


def custom_lookup_only_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--custom_lookup_only",
        action="store_true",
        help="Only use the lookup file for ASN description (default: both)",
        default=False,
    )


def prefix_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--prefixes",
        metavar="",
        type=int,
        help="Number of prefixes to process in mrt file (default:all)",
        default=None,
    )


def quiet_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--quiet",
        action="store_true",
        help="Turn off verbose (default:verbose)",
        default=False,
    )


def ipaddress_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--ipaddress", metavar="", type=str, help="IP address lookup", default=""
    )


def asn_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--asn", metavar="", type=str, help="ASN lookup", default=""
    )


def bgpscan_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--bgpscan",
        action="store_true",
        help="Using faster bgpscanner to parse mrt file",
        default=False,
    )


def display_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--display",
        action="store_true",
        help="Display the database",
        default=False,
    )


def trim_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--trim",
        nargs='*',
        help="Trim the database by providing the key(s) to be removed from the json data",
        default=False,
    )


def prometheus_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--prometheus",
        action="store_true",
        help="Output statistics for prometheus injestion",
        default=False,
    )


def database_type_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--database_type",
        metavar="",
        type=str,
        help="Type pf mmdb database (default: mrt2mmdb)",
        default="mrt2mmdb",
    )


def show_db_type_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--show_db_type",
        action="store_true",
        help="Show the mmdb database type",
        default=False,
    )


def log_level_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--log_level",
        metavar="",
        type=str,
        help="logging level [CRITICAL|WARNING|INFO|DEBUG](default: WARNING)",
        default="WARNING",
    )


def compare_routing_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--compare_routing",
        metavar="",
        type=str,
        nargs="*",
        help="Compare routing differenceis between [filename1] [filename2] \
              (both files are in mmdb format)",
    )


def print_changes_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--print_changes",
        action="store_true",
        help="Print changes when comparing [filename1] [filename2]",
        default=False,
    )


def compare_asn_arg(parser):
    """define arguments to be added"""
    return parser.add_argument(
        "--compare_asn",
        action="store_true",
        help="Compare the ASN in a mmdb file vs tsv,csv file [filename1] [filename2]",
        default=False,
    )


def get_args(options):
    """
    Input: keywords boolean of the desire argument
    Output: The argument obtained from the command line
    Caveat: Boolean variable allows the desired argument to be captured
    """
    parser = argparse.ArgumentParser()
    for arg in options:
        arg(parser)
    return parser


def main():
    """
    main function define to test the function before integeration
    """
    parser = get_args([mmdb_arg, mrt_arg, log_level_arg])
    args = parser.parse_args()
    del args
    return 0


if __name__ == "__main__":
    main()
