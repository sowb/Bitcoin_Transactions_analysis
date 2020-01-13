"""Write the nodes and the links of the Bitcoin Graph.

Write 3 types of nodes in 3 csv files: nodes of actors, 
nodes of transactions data, and nodes of addresses.

Write 2 types of relationships in 2 csv files:
relationships between the addresses_ids and the actors
relationships between transactions and addresses

The obtained files are stored in the working directory.

Data required in the working directory : The dict of 
addresses, the dict of all_clusters, the dict of names.

"""
# Import necessary modules
from functions_for_bitcoin_graph import *
from functions_write_save_load_data import get_files, load_dumped_data, write_to_csv
from functions_extract_transform_data import GetInTransaction, force_rewrite


def main():
    # Load the filenames
    all_files = get_files()
    # load the dictionary of addresses
    addresses_ids = load_dumped_data("addresses_ids.pickle", "pickle")
    # load the dictionary of clusters
    all_clusters = load_dumped_data("all_clusters.pickle", "pickle")
    # if you want use identified labels : uncomment the following line and 
    #load the dictionary of labels 
    #ids_names = load_dumped_data("dictionary_of_btc_names_int.pickle", "pickle")
    # Write the addresses nodes in a csv file
    write_to_csv(
        "nodes_addresses.csv",
        "w",
        nodes_links_using_dic(addresses_ids, dic_names=None, name_of_nodes="address "),
        header=[
            "addressId:ID(Addresses)",
            "name",
            "bitcoin_address",
            "actor_identity",
        ],
    )
    print("ADDRESSE'S NODES SAVED IN CSV")

    # Write the transactions nodes in a csv file
    get = GetInTransaction()

    write_to_csv(
        "nodes_transactions_data.csv",
        "w",
        nodes_transactions(
            all_files,
            "Tx ",
            get.txid,
            get.exchange_rate,
            get.timestamp,
            get.total_value,
        ),
        header=[
            "transactionId:ID(Transactions)",
            "exchange_rate",
            "timestamp",
            "total_value",
            "name",
        ],
    )
    print("TRANSACTIONS NODES SAVED IN CSV")

    # Write the actors nodes in a csv file
    write_to_csv(
        "nodes_actors.csv",
        "w",
        create_nodes_actors(all_clusters, name_of_node="actor "),
        header=["actorsId:ID(Actors)", "name", "cluster_size"],
    )
    print("ACTORS NODES SAVED IN CSV")

    # Write rels between the transactions and the addresses in a csv file
    write_to_csv(
        "relationships_transactions_addresses.csv",
        "w",
        create_links_tx_addresses(all_files, addresses_ids),
        header=[
            ":END_ID(Transactions)",
            "Tx_INS_or_OUTS",
            "value",
            "hashPrevOut_or_indexOut",
            ":START_ID(Addresses)",
        ],
    )
    print("LINKS BETWEEN TRANSACTIONS AND ADDRESSES SAVED IN CSV")

    # Write rels between the addresses and the actors in a csv file
    write_to_csv(
        "relationships_addressesids_actors.csv",
        "w",
        nodes_links_using_dic(all_clusters, dic_names=None),
        header=[":START_ID(Addresses)", "actor_identity", ":END_ID(Actors)"],
    )
    print("LINKS BETWEEN ADDRESSES AND ACTORS SAVED IN CSV")


if __name__ == "__main__":
    main()
