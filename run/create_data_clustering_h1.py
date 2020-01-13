"""Create the data for the addresses clustering. 

Running this script will create the dictionary of
addresses, save the input/output addresses in 
integers and strings. Apply the heuristic 1 (input
address) and write the nodes and the relationships
in csv files for the clustering graph used in Neo4j.

"""
from functions_write_save_load_data import *
from functions_check_in_data import size_of_file, size_of_variable
from functions_extract_transform_data import (
    addresses_to_dictionary,
    invert_dictionary,
    GetInTransaction,
    rewrite,
)

from functions_for_clustering import change_address, generate_nodes, generate_rels


def main():
    print("Doing the clustering of BTC addresses with only Heuristic 1...")

    get = GetInTransaction()
    all_files = get_files()
    # Load data select all unique addresses in a transaction and save
    load_apply_save(all_files, get.unique_addresses, "all_addresses")
    # reload the row by row, create dic and dump in one file
    addresses_to_dictionary("all_addresses", "ids_addresses.pickle", "pickle")
    size_of_file(["ids_addresses.pickle"])

    # inversion of the dictionary, for the following task we need
    # addresses as key
    ids_addresses = load_dumped_data("ids_addresses.pickle", "pickle")
    addresses_ids = invert_dictionary(ids_addresses)
    #  addresses_ids["Unable_to_decode_address"] = addresses_ids.pop('')
    dump_variable(addresses_ids, "addresses_ids.pickle", "pickle")

    # get out addresses_ids from memory
    # reset_selective - f addresses_ids

    # load transactions files, get only input addresses and save them
    load_apply_save(all_files, get.input_addresses, "input_addresses_str")

    # list of lists output addresses
    load_apply_save(all_files, get.output_addresses, "output_addresses_str")

    # Reload string adds in input, convert them to num
    reload_apply_save(
        "input_addresses_str",
        "input_addresses_int",
        rewrite,
        addresses_ids,
        pattern_to_remove="",
    )

    # Reload input addresses in string format, convert them to integer
    reload_apply_save(
        "output_addresses_str",
        "output_addresses_int",
        rewrite,
        addresses_ids,
        pattern_to_remove="",
    )

    # Write nodes in csv for neo4j usage
    write_to_csv(
        "clustering_nodes.csv",
        "w",
        generate_nodes("input_addresses_int"),
        header=["addressId:ID(Addresses)", "name"],
    )
    # res : 119 612 826 de nodes

    # Write links in csv for neo4j usage
    write_to_csv(
        "clustering_relationships.csv",
        "w",
        generate_rels("input_addresses_int"),
        header=[":START_ID(Addresses)", ":END_ID(Addresses)"],
    )
    # res : 126 170 166 de liens
    # runtime : 1h 21min 15s


if __name__ == "__main__":
    main()
