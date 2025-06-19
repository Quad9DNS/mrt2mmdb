
# mrt2mmdb

Scripts to convert [Multi-Threaded Routing Toolkit](https://datatracker.ietf.org/doc/html/rfc6396) files into [Maxmind database files](https://maxmind.github.io/MaxMind-DB). 

The goal of this project is to generate a mmdb file enriching it with information from MRT files or another Maxmind database file or custom csv,tsv file. The newly generated mmdb file will extract relevent fields from the various sources and store the information inside the mmdb file associating these values with the IP prefixes.

Maxmind database file offers fast lookup on IP prefixes/IP address and returns the associative values accordingly. 

In this project, the values we stored after conversion from MRT file into a Maxmind database file are Autonomous System Number (ASN), ASPATH and the corresponding description of the ASN. The description of ASN can be obtain from [Maxmind database](https://github.com/P3TERX/GeoLite.mmdb) while the ASPATH can be obtain from the MRT file. 


The mmdb file created will be used by other applications for IP address/prefix lookup.
## Installation

NOTE: Requires special version of the MaxMind DB Reader - first perform "pip install git+https://github.com/sbng/MaxMind-DB-Reader-python.git"

Install with python pip install

```bash
  git clone https://github.com/Quad9DNS/mrt2mmdb.git
  cd mrt2mmdb
  pip install -e .
```
mrt2mmdb script will be created and installed within the executable path of the system. The script needs a minimum input of a MRT file, mmdb file or csv/tsv file. The script offer multiple arguments and options to yeild the desire output. A target filename of the mmdb file must always be included in order to write the mmdb file.
 
```bash
$ mrt2mmdb -h                                                                                      
usage: mrt2mmdb [-h] [--mrt] [--mmdb] [--prefixes] [--target] [--lookup_file] [--custom_lookup_only] [--quiet] [--bgpscan] [--prometheus]
                [--database_type] [--log_level]

optional arguments:
  -h, --help            show this help message and exit
  --mrt                 Filename of mrt dump
  --mmdb                Filename of Maxmind mmdb file for prefixes lookup and return description/ASN
  --prefixes            Number of prefixes to process in mrt file (default:all)
  --target              Filename of new target mmdb file generated from mmrt file
  --lookup_file         Filename of csv,tsv file for custom ASN lookup
  --custom_lookup_only  Only use the lookup file for ASN description (default: both)
  --quiet               Turn off verbose (default:verbose)
  --bgpscan             Using faster bgpscanner to parse mrt file
  --prometheus          Output statistics for prometheus injestion
  --database_type       Type pf mmdb database (default: mrt2mmdb)
  --log_level           logging level [CRITICAL|WARNING|INFO|DEBUG](default: WARNING)
```

    
## Usage/Examples
Bare minimum execution of the script. In this example, a MRT and mmdb file is provided to generate the target mmdb file. The newly generated mmdb file will contain ASPATH (from MRT file) and AS description (from mmdb file). 
```bash
$ mrt2mmdb --mrt ../mke-20240329.mrt --mmdb GeoLite2-ASN.mmdb --target target.mmdb     
 Making ASN table for description lookup GeoLite2-ASN.mmdb                         : 625093 prefixes [00:07, 78835.80 prefixes/s]
 Making custom ASN table using lookup file                                         : skipped
 Loading mrt data into dictionary using ../mke-20240329.mrt                        : 934 prefixes [00:00, 5530.98 prefixes/s]
 Converting mrt into mmda target.mmdb                                              : 926 prefixes [00:00, 3621.76 prefixes/s]
 Writing mmda file target.mmdb                                                     : 1 [00:00, 12.13/s]
 Prefixes without description                                                      : 13 prefixes
 ASN without description                                                           : 3 prefixes
```

A set of scripts (lookup.py, difference.py, filter.py, geoblur.py) are available for diagnostic purposes and modification of mmdb file. These scripts allow the user to investigate the content of the generated mmdb file to ensure correctness of the data (lookup.py and difference.py). The filter.py script could be use to update and filter keys from existing mmdb file. In the previous mmdb file generated (target.mmdb), the user can investigate the content of target.mmdb. The geoblur.py script can be used to apply "geographical blurring" to the database.

```bash
:$ ./lookup.py --help                                      
usage: lookup.py [-h] [--mmdb] [--ipaddress] [--asn] [--display] [--show_db_type]

optional arguments:
  -h, --help      show this help message and exit
  --mmdb          Filename of Maxmind mmdb file for prefixes lookup and return description/ASN
  --ipaddress     IP address lookup
  --asn           ASN lookup
  --display       Display the database
  --show_db_type  Show the mmdb database type

$ ./lookup.py --mmdb target.mmdb --display | jq | tail -10                                   
  [
    "4000::/2",
    {
      "autonomous_system_number": 42,
      "autonomous_system_organization": "WOODYNET-1",
      "prefix": "::/0",
      "path": "42"
    }
  ]
]
```
diffference.py check the difference between mmdb and csv,tsv files. This helps to identify the discrepancies.

```bash
./difference.py --compare_asn --mmdb data/GeoLite2-ASN.mmdb --lookup data/asn_rir_org_country.csv
 Making ASN table for description lookup data/GeoLite2-ASN.mmdb                    : 625093 prefixes [00:07, 78685.91 prefixes/s]
 Making custom ASN table using lookup file data/asn_rir_org_country.csv            : 143735 prefixes [00:00, 240479.55 prefixes/s]
DeepDiff 1 seconds in progress. Pass #0, Diff #14883
DeepDiff 2 seconds in progress. Pass #0, Diff #56687
DeepDiff 3 seconds in progress. Pass #0, Diff #132981
DeepDiff 4 seconds in progress. Pass #0, Diff #197986
dictionary_item_added = 66861
dictionary_item_removed = 50
values_changed = 72540
```
filter.py filter the keys of an existing mmdb file. This will reduce the size and data section of the mmdb file without changing the binary search tree. Essentially it trims the mmdb file and get rid of the specified keys in the data section associated to each prefix.
```bash
$ ./filter.py --help                                                                                                                                             [ 3:43PM]
usage: filter.py [-h] [--mmdb] [--trim [TRIM ...]] [--quiet]

options:
  -h, --help         show this help message and exit
  --mmdb             Filename of Maxmind mmdb file for prefixes lookup and return description/ASN
  --trim [TRIM ...]  Trim the database by providing the key(s) to be removed from the json data
  --quiet            Turn off verbose (default:verbose)
```

geoblur.py can apply "geographical blurring" to the database, moving elements pointing to low population locations to nearest locations with higher population.
```bash
$ ./geoblur.py --help

usage: geoblur.py [-h] [--mmdb ] [--geonames_cities ] [--admincodes ] [--min_population ] [--database_type ] [--target ]
                  [--quiet] [--log_level ]

options:
  -h, --help          show this help message and exit
  --mmdb              Filename of Maxmind mmdb file for prefixes lookup and return description/ASN
  --geonames_cities   Filename of geonames cities file for cities population lookup(e.g. txt file from
                      https://download.geonames.org/export/dump/cities15000.zip)
  --admincodes        Filename of geonames adminc1codes file (e.g.
                      https://download.geonames.org/export/dump/admin1CodesASCII.txt)
  --min_population    Minimal population count allowed in dataset cities(default: 15000)
  --database_type     Type pf mmdb database (default: mrt2mmdb)
  --target            Filename of new target mmdb file generated from mmrt file
  --quiet             Turn off verbose (default:verbose)
  --log_level         logging level [CRITICAL|WARNING|INFO|DEBUG](default: WARNING)


# Example blurring locations with population < 15000
$ wget https://download.geonames.org/export/dump/cities15000.zip
Saving 'cities15000.zip'
HTTP response 200 OK [https://download.geonames.org/export/dump/cities15000.zip]

$ unzip cities15000.zip
Archive:  cities15000.zip
  inflating: cities15000.txt

$ wget https://download.geonames.org/export/dump/admin1CodesASCII.txt
Saving 'admin1CodesASCII.txt'
HTTP response 200 OK [https://download.geonames.org/export/dump/admin1CodesASCII.txt]

$ ./geoblur.py --geonames_cities cities15000.txt --mmdb target.mmdb --target target.blurred.mmdb --admincodes admin1CodesASCII.txt --min_population 15000
Reading cities data from cities15000.txt
Grouping and filtering cities data
Reading admincodes data from admin1CodesASCII.txt
Blurring location data from target.mmdb and writing to target.blurred.mmdb
```


```bash
# Demo. A simple target mmdb file with data section comprising of single key 'network'
$./lookup.py --mmdb target.mmdb --display
[
 [
  "1.0.0.0/24",
  {
   "network": "1.0.0.0/24"
  }
 ],
 [
  "1.0.1.0/24",
  {
   "network": "1.0.1.0/24"
  }
 ]
]

# Remove the 'network' key from the data section while a new mmdb file target.mmdb.trim are generated with the relevant updates and modification. Original mmdb file remain intact.
$ ./filter.py --mmdb target.mmdb --trim network 
 Apply filter to trim mmdb file: 2 prefixes [00:00, 4534.38 prefixes/s]
$ ./lookup.py --mmdb target.mmdb.trim --display 
[
 [
  "1.0.0.0/24",
  {}
 ],
 [
  "1.0.1.0/24",
  {}
 ]
]
```

## Extra Arguments

Other useful feature is to disable the lookup of mmdb file and rely solely on the the csv,tsv file by using the --custom_lookup_only switch. The default lookup is to combine both the csv,tsv and mmdb file as lookup table. If the same entries exist in both sources, the csv,tsv file will be the source of truth.

```bash
$ mrt2mmda --mrt mke-20240329.mrt --lookup_file asn_rir_org.tsv --target target1.mmdb --custom_lookup_only
 Making ASN table for description lookup                                           : skipped
 Making custom ASN table using lookup file data/asn_rir_org.tsv                    : 143723 prefixes [00:00, 251780.41 prefixes/s]
 Loading mrt data into dictionary using mke-20240329.mrt                           : 934 prefixes [00:00, 5682.11 prefixes/s]
 Converting mrt into mmda target1.mmdb                                             : 926 prefixes [00:00, 3594.89 prefixes/s]
 Writing mmda file target1.mmdb                                                    : 1 [00:00, 11.61/s]
 Prefixes without description                                                      : 0 prefixes
 ASN without description                                                           : 0 prefixes
```

Occasionally, user may have a large MRT file (1+ million entries) as input, the argument --prefix <num> can be use to reduce the processing time by only parsing <num> entries. This will greatly reduce the time for troubleshooting. 

```bash
$ mrt2mmdb --mrt mke-20240329.mrt --lookup_file asn_rir_org.tsv --target target1.mmdb --custom_lookup_only --prefix 100
 Making ASN table for description lookup                                           : skipped
 Making custom ASN table using lookup file data/asn_rir_org.tsv                    : 143723 prefixes [00:00, 258034.86 prefixes/s]
 Loading mrt data into dictionary using mke-20240329.mrt                           : 100 prefixes [00:00, 5483.47 prefixes/s]
 Converting mrt into mmda target1.mmdb                                             : 99 prefixes [00:00, 3347.19 prefixes/s]
 Writing mmda file target1.mmdb                                                    : 1 [00:00, 67.74/s]
 Prefixes without description                                                      : 0 prefixes
 ASN without description                                                           : 0 prefixes
```

mrt2mmdb script can also operate in silent mode using the --quiet argument. This will surpress all output and generate the target mmdb only. Silent mode is useful while running as automated script where output is irrelevent.

mrt2mmdb script can also be use to generate prometheus formatted output. This allows the output to be injested by prometheus. By default, --quiet mode is enforce when --prometheus option is selected and only prometheus injestable output will be generated (as well as the target mmdb file)

```bash
$ mrt2mmdb --mrt mke-20240329.mrt --lookup_file data/asn_rir_org.tsv --target target1.mmdb --custom_lookup_only --prometheus
#
# durations are seconds
mrt2mmdb_description_asn_prefixes 143723
mrt2mmdb_description_asn_prefixes_duration 0
mrt2mmdb_description_asn_prefixes_per_second 321179.00
#
mrt2mmdb_dictionary_load_prefixes 934
mrt2mmdb_dictionary_load_prefixes_duration 0
mrt2mmdb_dictionary_load_prefixes_per_second 5959.02
#
mrt2mmdb_conversions 926
mrt2mmdb_conversions_duration 0
mrt2mmdb_conversions_per_second 2765.24
#
# How many prefixes were not found in the Maxmind template file that we’re using as a source for names?
#
mrt2mmdb_prefixes_no_description 0
mrt2mmdb_asn_no_description 0
#
# When did this instance of the process start? Unix epoch seconds.
#
mrt2mmdb_lastrun_timestamp 0
#
# This is the creationtime of the MRT file that is being parsed
# Unix epoch seconds.  This is what we can use to see if somehow
# our MRT file collection pipeline is “stuck” and not being updated.
#
mrt2mmdb_mrt_file_creation_timestamp 1711908182
#
# This is the creationtime of the template MMDB file that is being parsed
# Unix epoch seconds.  This is what we can use to see if somehow
# our template MMDB file collection pipeline is “stuck” and not being updated.
#
mrt2mmdb_template_mmdb_file_creation_timestamp 1712132576
#
# Keep a version number so we can track behaviors of different variations
# MUST BE NUMERIC ONLY, with a single decimal point.
#
mrt2mmdb_version 1.0
```
## Contribution
Original Idea: John Todd <jtodd>

Author : Seo Boon Ng <seo.boon.ng at gmail.com>
