#!/usr/bin/env python
""" This module output prometheus formated text for prometheus injestion.
Require statistic from various sources to include in the prometheus output.
"""


def output_prometheus(
    asn_stats, prefix_stats, convert_stats, missing_stats, files_stats
):
    """Return the prometheus format output using a f-string templating"""
    return f"""#
# HELP mrt2mmdb_description_asn_prefixes number of prefixes
# TYPE mrt2mmdb_description_asn_prefixes gauge
mrt2mmdb_description_asn_prefixes {asn_stats[0]}
# HELP mrt2mmdb_description_asn_prefixes_duration duration of prefix load
# TYPE mrt2mmdb_description_asn_prefixes_duration gauge
mrt2mmdb_description_asn_prefixes_duration {asn_stats[1]:.0f}
# HELP mrt2mmdb_description_asn_prefixes_per_second number of prefixes processed per second
# TYPE mrt2mmdb_description_asn_prefixes_per_second gauge
mrt2mmdb_description_asn_prefixes_per_second {asn_stats[0]/asn_stats[1]:.2f}
# HELP mrt2mmdb_dictionary_load_prefixes number of dictionary prefixes loaded
# TYPE mrt2mmdb_dictionary_load_prefixes gauge
mrt2mmdb_dictionary_load_prefixes {prefix_stats[0]}
# HELP mrt2mmdb_dictionary_load_prefixes_duration total duration of dictionary prefix loading
# TYPE mrt2mmdb_dictionary_load_prefixes_duration gauge
mrt2mmdb_dictionary_load_prefixes_duration {prefix_stats[1]:.0f}
# HELP mrt2mmdb_dictionary_load_prefixes_per_second dictionary prefixes processed per second
# TYPE mrt2mmdb_dictionary_load_prefixes_per_second gauge
mrt2mmdb_dictionary_load_prefixes_per_second {prefix_stats[0]/prefix_stats[1]:.2f}
# HELP mrt2mmdb_conversions number of conversions
# TYPE mrt2mmdb_conversions gauge
mrt2mmdb_conversions {convert_stats[0]}
# HELP mrt2mmdb_conversions_duration duration of conversions
# TYPE mrt2mmdb_conversions_duration gauge
mrt2mmdb_conversions_duration {convert_stats[1]:.0f}
# HELP mrt2mmdb_conversions_per_second conversions per second
# TYPE mrt2mmdb_conversions_per_second gauge
mrt2mmdb_conversions_per_second {convert_stats[0]/convert_stats[1]:.2f}
# HELP mrt2mmdb_prefixes_no_description prefixes with no description text found
# TYPE mrt2mmdb_prefixes_no_description gaugte
mrt2mmdb_prefixes_no_description {len(missing_stats)}
# HELP mrt2mmdb_asn_no_description asns with no description text found
# TYPE mrt2mmdb_asn_no_description gauge
mrt2mmdb_asn_no_description {len(set(missing_stats))}
# HELP mrt2mmdb_lastrun_timestamp epoch timestamp of process start
# TYPE mrt2mmdb_lastrun_timestamp gauge
mrt2mmdb_lastrun_timestamp {files_stats[0]:.0f}
# HELP mrt2mmdb_mrt_file_creation_timestamp epoch timestamp of mrt file creation time
# TYPE mrt2mmdb_mrt_file_creation_timestamp gauge
mrt2mmdb_mrt_file_creation_timestamp {files_stats[1]:.0f}
# HELP mrt2mmdb_template_mmdb_file_creation_timestamp epoch timestamp of mmdb template file if present
# TYPE mrt2mmdb_template_mmdb_file_creation_timestamp gauge
mrt2mmdb_template_mmdb_file_creation_timestamp {files_stats[2]:.0f}
# HELP mrt2mmdb_version version number of mrt2mmdb code
# TYPE mrt2mmdb_version gauge
mrt2mmdb_version 1.0
"""


def main():
    """
    main function define the workflow to make a ASN dict->Load the
    corresponding mrt->convert the mrt into mmda
    """
    return 0


if __name__ == "__main__":
    main()
