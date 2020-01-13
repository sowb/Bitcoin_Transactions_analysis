"""Use the Neo4j partitions and create the dictionary of the clusters
    
Charge the file containing the partitions exported from neo4j,
Transform the partitions to a dictionary, in which the keys 
are the partitions id's, the values are the addresses (in integers).
The dictionary is sorted, the first keys are the biggest clusters.
 
In this script we also add the unique addresses in input/output 
transactions to the clusters

"""
from functions_write_save_load_data import load_dumped_data
from functions_check_in_data import size_of_file, size_of_variable
from functions_for_clustering import build_save_clusters, append_rest_of_clusters


def main():
    print(
        "starting to construct the clusters (a list of list) using neo4j_partitions..."
    )
    build_save_clusters("neo4j_partitions.csv", "clusters.pickle", "pickle")

    # add the rest of the clusters : unique addresses in input
    print("starting to add to the clusters unique addresses in input :")
    append_rest_of_clusters(
        "input_addresses_int",
        list,
        "clusters.pickle",
        "clusters_w_unique_outs.pickle",
        "pickle",
    )

    # add the rest of the clusters : using the dictionary of adds
    print("starting to add to the clusters unique addresses in output")

    append_rest_of_clusters(
        "addresses_ids.pickle",
        dict,
        "clusters_w_unique_outs.pickle",
        "all_clusters.pickle",
        "pickle",
    )

    # load  clusters
    all_clusters = load_dumped_data("all_clusters.pickle", "pickle")
    print("The number of clusters is", len(all_clusters))

    # Check the size of the all the clusters
    size_of_file(["all_clusters.pickle"])


if __name__ == "__main__":
    main()
