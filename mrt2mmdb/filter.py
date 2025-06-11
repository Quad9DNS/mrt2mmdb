#!/usr/bin/env python
"""
utility to help lookup on description of network base on IP address or ASN
"""
import os
import sys
import ipaddress
import struct
import shutil
from tqdm import tqdm
import maxminddb
from maxminddb.reader import Reader
from mmdb_encoder import Encoder

from args import (
    get_args,
    mmdb_arg,
    trim_arg,
    quiet_arg,
)


# pylint: disable=global-statement
args = {}


def decode_pointer(res, reader):
    """
    This function will decode the data section pointer and return a data offset
    for used in the leaf node. The return needs to be 4 bytes as we are dealing with
    a 32 bites nodes
    """
    if len(res) == 5:
        _, pointer = struct.unpack(">BI", res)
    elif len(res) == 4:
        b0, b1, b2, b3 = struct.unpack(">BBBB", res)
        pointer = (b0 & 0x07) << 24 | b1 << 16 | b2 << 8 | b3
        pointer += 526336
    elif len(res) == 3:
        b0, b1, b2 =  struct.unpack(">BBB", res)
        pointer = (b0 & 0x07) << 16 | b1 << 8 | b2
        pointer += 2048
    elif len(res) == 2:
        b0, b1 = struct.unpack(">BB", res)
        pointer = (b0 & 0x07) << 8 | b1
    else:
        raise ValueError("Invalid encoded pointer")
    return struct.pack(">I", pointer + reader._metadata.node_count + 16)

def filter_dict(raw):
    """
    filter and remove keys from dictionary. The keys to be removed are store in ignore_keys and ignore_lang
    list
    """
    ignore_keys = []
    ignore_lang = ["de", "es", "fr", "ja", "pt-BR", "ru", "zh-CN"]
    if args.trim:
        rem_keys = ignore_keys + ignore_lang + args.trim
    else:
        rem_keys = ignore_keys + ignore_lang

    fnc = lambda sub: (
        [
            fnc(item) if isinstance(item, dict) else item
            for item in sub
            if isinstance(sub, list)
        ]
        if isinstance(sub, list)
        else {
            key1: fnc(val1) if isinstance(val1, (dict, list)) else val1
            for key1, val1 in sub.items()
            if key1 not in rem_keys
        }
    )
    return fnc(raw)


def load_db(fname):
    """
    Load mmdb file and use it to create a generator expression to
    avoid loading the entire structure into the memory. This function 
    will return a list of prefix/dictionaries generator expression
    """
    result = []
    mreader = maxminddb.open_database(fname)
    res = (((prefix.compressed), filter_dict(data)) for prefix, data in mreader)
    mreader.close
    return res


def rewrite(fname, dic_data, count, target_name):
    shutil.copyfile(fname, target_name)
    with open(target_name, "r+b") as fh:
        with Reader(fname) as reader:
            metadata = reader.metadata()
            treesize = int(((metadata.record_size * 2) / 8) * metadata.node_count)
            data_section_start = treesize + 16
            data_section_end = reader._buffer.rfind(
                reader._METADATA_START_MARKER, max(0, reader._buffer_size - 128 * 1024)
            )
    
            resolved = data_section_start
            metadata_cache = reader._buffer[data_section_end:]
            encode_record = Encoder(cache=True)
            for prefix, record in dic_data:
                a = prefix.split("/")[0]
                address = bytearray(ipaddress.ip_address(a).packed)
                """
                Encode the dictionary record using the Encode Record Object. Cache is set to
                True. This ensure that repeated data (strings, float, integers) are referenced 
                by pointers instead of re-encoding the same data. This save bytes. We also
                Use the return (data pointer) from the Encode Record Object, for locating the
                relevent search tree node. We can update the leaf node pointer to this new encoded
                data as the data had changed.
                """
                pack = encode_record.encode(record)
                pack_pointer = decode_pointer(pack, reader)
                """
                Update the search tree leaf node with the pointer of the data for this node
                using the custom find_address_in_tree_loc function from Decoder module. The 
                function return -> the address of the location of leaf node. 
                Result :      Tuple of location of leaf node (loc) and the pointer 
                              (data_pointer) to the data. 
                """
                ((_, loc), _) = reader._find_address_in_tree_loc(
                    address
                )
                fh.seek(loc)
                fh.write(pack_pointer)
                """
                extract the data content from the Encoded Record Object. Once extract the required data
                zero out the data list. This is to ensure the Encoded Record Object remain lean. However,
                we need to ensure this data is written to the mmdb file.
                """
                encode_data = encode_record.data_list
                data_bytes = b"".join([bytes_obj for bytes_obj in encode_data])
                fh.seek(resolved)
                fh.write(data_bytes)
                resolved += len(data_bytes)
                encode_record.data_list = []
                count.update(1)
            fh.write(metadata_cache)
            fh.truncate(fh.tell())


def main():
    """
    main function for the workflow
    """
    parser = get_args(
        [mmdb_arg,trim_arg,quiet_arg]
    )
    global args
    args = parser.parse_args()
    if not os.path.isfile(args.mmdb):
        parser.print_help(sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
    fname = args.mmdb
    with tqdm(
             desc=f" {'Apply filter to trim mmdb file': <80}  ",
            unit=" prefixes",
            disable=args.quiet
        ) as pb:
        db = load_db(fname)
        rewrite(fname, db, pb, fname + ".trim")
