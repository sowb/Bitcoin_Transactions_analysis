"""Apply the change heuristic to data. 

Select the change addresses in the data and 
save it on hard drive. Then create and append
the nodes and the relationships to heuristic 1.

"""
from functions_write_save_load_data import write_to_csv, load_dumped_data, get_files
from functions_check_in_data import size_of_file, size_of_variable
from functions_for_clustering import change_address, generate_nodes, generate_rels


def main():
    all_files = get_files()

    print("Applying the change address heuristic to the data...")
    addresses_ids = load_dumped_data("addresses_ids.pickle", "pickle")
    change_address(all_files, "change_addresses", addresses_ids)
    # Size of the list of change addresses
    size_of_variable(["change_addresses"])
    # Write nodes in csv for neo4j : append to h1 nodes
    write_to_csv(
        "clustering_nodes.csv", "a", generate_nodes("change_addresses"), header=[]
    )
    # Write links in csv for neo4j : append to h1 links
    write_to_csv(
        "clustering_relationships.csv",
        "a",
        generate_rels("change_addresses"),
        header=[],
    )


if __name__ == "__main__":
    main()
