#!/usr/bin/env python
"""
This module runs "geographic blurring" of the maxmind database. This hides
locations that have a too low population and merges them with nearby locations,
to anonymize the data.
"""
import csv
import logging
import os
import shutil
import sys
from math import asin, cos, radians, sin, sqrt
from operator import itemgetter

import maxminddb
from tqdm import tqdm

from args import (admincodes_arg, database_type_arg, geonames_cities_arg,
                  get_args, log_level_arg, min_population_arg, mmdb_arg,
                  quiet_arg, target_arg)
from filter import rewrite


# Taken from: https://stackoverflow.com/a/4913653
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance in kilometers between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    # Radius of earth in kilometers. Use 3956 for miles.
    # Determines return value units.
    r = 6371
    return c * r


# pylint: disable=global-statement
args = {}

adm1codes_usage = ["US", "CH", "BE", "ME"]


def parse_geonames_cities(
        fname, quiet, min_population
) -> dict[str, list[dict]]:
    """
    Parse geonames cities file
    Groups into lists of cities per country code and admin1 code
    Based on fields from: https://download.geonames.org/export/dump/
    """
    fieldnames = [
        'geonameid', 'name', 'asciiname', 'alternatenames', 'latitude',
        'longitude', 'feature class', 'feature code', 'country code',
        'cc2', 'admin1 code', 'admin2 code', 'admin3 code', 'admin4 code',
        'population', 'elevation', 'dem', 'timezone', 'modification date'
    ]
    file = open(fname, 'r')
    message = "Reading cities data from " + fname

    cities = []
    with tqdm(
        desc=f" {message: <80}  ",
        unit=" lines",
        disable=args.quiet,
    ) as pb:
        def clean_up(x):
            x['longitude'] = float(x['longitude'])
            x['latitude'] = float(x['latitude'])
            x['population'] = int(x['population'])
            x['geonameid'] = int(x['geonameid'])
            pb.update(1)
            return x

        cities = [
            clean_up(x)
            for x in csv.DictReader(
                file,
                fieldnames=fieldnames,
                delimiter="\t"
            )
        ]

    message = "Grouping and filtering cities data"
    grouped = {}
    with tqdm(
        desc=f" {message: <80}  ",
        unit=" cities kept and grouped",
        disable=args.quiet,
    ) as pb:
        for city in cities:
            if int(city['population']) < min_population:
                continue

            key = city['country code']
            if 'admin1 code' in city and key in adm1codes_usage:
                key = f"{city['country code']}.{city['admin1 code']}"

            if not grouped.get(key):
                grouped[key] = []

            grouped[key].append(city)
            pb.update(1)

    return grouped


def parse_admincodes(fname, quiet) -> dict[str, dict]:
    """
    Parse admincodes file
    Maps admin1 code to object
    """
    fieldnames = [
        'code', 'name', 'asciiname', 'geonameid'
    ]
    file = open(fname, 'r')
    message = "Reading admincodes data from " + fname

    admincodes = []
    with tqdm(
        desc=f" {message: <80}  ",
        unit=" lines",
        disable=args.quiet,
    ) as pb:
        def clean_up(x):
            pb.update(1)
            x['geonameid'] = int(x['geonameid'])
            return x

        admincodes = [
            clean_up(x)
            for x in csv.DictReader(
                file,
                fieldnames=fieldnames,
                delimiter="\t"
            )
        ]

    grouped = {}
    for admincode in admincodes:
        grouped[admincode['code']] = admincode

    return grouped


def get_iso_code(data):
    country = data.get('country')
    if country:
        return country.get('iso_code')
    return None


def get_full_iso_code(data):
    iso_code = get_iso_code(data)
    if iso_code:
        if iso_code in adm1codes_usage:
            subdivisions = data.get('subdivisions')
            if subdivisions and len(subdivisions) > 0:
                subdivision_iso_code = subdivisions[0]['iso_code']
                if subdivision_iso_code:
                    return f"{iso_code}.{subdivision_iso_code}"
        return iso_code
    return None


def blurred_generator(mreader, cities, admincodes, args, cities_to_update):
    for prefix, data in mreader:
        if 'city' in data and 'geoname_id' in data['city']:
            if data['city']['geoname_id'] not in cities_to_update:
                yield (prefix.compressed, data)
                continue

        same_country_cities = (cities.get(get_full_iso_code(data))
                               if get_full_iso_code(data) else [])

        if not same_country_cities:
            same_country_cities = (cities.get(get_iso_code(data))
                                   if get_iso_code(data) else [])

        if not same_country_cities:
            same_country_cities = []

        lat = None
        lon = None
        if 'location' in data:
            lat = data['location']['latitude']
            lon = data['location']['longitude']
            del data['location']
        if 'city' in data:
            del data['city']
        if 'subdivisions' in data:
            del data['subdivisions']

        if lat and lon and len(same_country_cities) > 0:
            for i in range(1, 101):
                max_dist = i * 5
                valid_cities = [
                    c
                    for c in same_country_cities
                    if haversine(
                        lon,
                        lat,
                        c['longitude'],
                        c['latitude']
                    ) < max_dist and c['population'] >
                    args.min_population
                ]
                valid_cities.sort(key=itemgetter('population'))
                if len(valid_cities) > 0:
                    found_city = valid_cities[0]
                    found_admincode = admincodes.get(
                        f"{found_city['country code']}."
                        f"{found_city['admin1 code']}"
                    )
                    data['location'] = {
                        'longitude': found_city['longitude'],
                        'latitude': found_city['latitude'],
                    }
                    data['city'] = {
                        'geoname_id': found_city['geonameid'],
                        'names': {'en': found_city['asciiname']}
                    }
                    data['subdivisions'] = [
                        {
                            'iso_code': found_city['admin1 code'],
                            'geoname_id': (
                                found_admincode['geonameid']
                                if found_admincode else 0),
                            'names': {'en':
                                      found_admincode['asciiname']}
                            if found_admincode else {}
                        }
                    ] if 'admin2 code' in found_city else []
                    break

        yield (prefix.compressed, data)


def main():
    parser = get_args(
        [
            mmdb_arg,
            geonames_cities_arg,
            admincodes_arg,
            min_population_arg,
            database_type_arg,
            target_arg,
            quiet_arg,
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

    if not os.path.isfile(args.mmdb):
        logger.warning("\nerror: Unable to locate mmdb file\n")
        parser.print_help(sys.stderr)
        sys.exit(1)

    if not os.path.isfile(args.geonames_cities):
        logger.warning("\nerror: Unable to locate geonames cities file\n")
        parser.print_help(sys.stderr)
        sys.exit(1)

    if args.quiet:
        logging.disable(logging.WARNING)
    logger.debug(args)

    cities = parse_geonames_cities(
        args.geonames_cities, args.quiet, args.min_population)
    admincodes = []
    if args.admincodes:
        admincodes = parse_admincodes(args.admincodes, args.quiet)

    cities_to_update = {city['geonameid']
                        for group in cities.values()
                        for city in group
                        if city['population'] < args.min_population}

    shutil.copyfile(args.mmdb, args.target)

    message = "Blurring location data from " + \
        args.mmdb + " and writing to " + args.target
    with tqdm(
        desc=f" {message: <80}  ",
        unit=" prefixes",
        disable=args.quiet,
    ) as pb:
        mreader = maxminddb.open_database(args.mmdb)
        mreader_gen = ((prefix, data) for prefix, data in mreader)
        mreader.close
        rewrite(
            args.mmdb,
            blurred_generator(mreader_gen, cities, admincodes,
                              args, cities_to_update),
            pb,
            args.target
        )

    return 0


if __name__ == "__main__":
    main()
