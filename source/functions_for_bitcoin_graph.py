
"""List of functions used create to the data for the BTC graph. 

The functions from this script is use to write the nodes and the
relationships for BTC user graph. The functions write the data in 
csv files on the hard drive. 

This script is imported as module in other scripts. The script 
must be stored in the working directory. 

"""
# Import dependencies
from functions_write_save_load_data import (
    dump_variable,
    load_row_by_row,
    load_dumped_data,
)
from functions_extract_transform_data import (
    invert_dictionary,
    GetInTransaction,
    rewrite,
)


def nodes_transactions(files, name_col=None, *functions):
    """Create the transactions nodes.

    Parameters
    ----------
    files: list
        List of the data files names (part-0001, ...)
    name_col: str, optional(default is none)
        Specifies the name of the node
    *functions : functions
        Functions used to create the node and its properties

    Yields
    ------
    list
        A list that will be written in a csv file, each
        element in the list will written in a different column
   
   """
    from itertools import chain

    k = 0
    for file in files:
        # Load a row
        for row in load_row_by_row(file):
            k += 1
            if k % 10000000 == 0:
                print(k, "transactions processed")
            res_row = []

            # apply the functions, each function is for a column
            for fn in functions:
                res_row.append(fn(row))

            if name_col:
                res_row.append([name_col + str(k)])

            res_row = list(chain.from_iterable(res_row))

            yield res_row


def create_links_tx_addresses(files, dictionary):
    """Create the relationships between the transactions and the addresses.

    This function extract the informations of the properties
    Parameters
    ----------
    files: list
        List of the data files names (part-0001, ...)
    dictionary: dict
        The dictionary of addresses, in which the addresses are keys.

    Yields
    ------
    list
        A list that will be written in a csv file, each element in
        the list will be written in a different column. Columns to
        obtain: txid, address in or out, value, index in or out,
        address in int.

   """
    from itertools import zip_longest, chain

    get = GetInTransaction()
    cnt = 0
    for file in files:
        cnt += 1
        print("Processing file", cnt)
        for row in load_row_by_row(file):
            transaction = row

            ## Addresses
            # Select input/output addresses and convert the strings to integers
            ads_in = get.input_addresses(transaction)
            rewrite(ads_in, dictionary)
            ads_out = get.output_addresses(transaction)
            rewrite(ads_out, dictionary)

            ## Link "in" or "out" and their corresponding indexes to each addresses
            #
            ins = list(zip_longest(ads_in, ["in"], fillvalue="in"))

            # get the indexes and a
            indexin = get.index_in(transaction)
            ins = list(zip(ins, indexin))
            try:
                indexout = get.index_out(transaction)
            except:
                indexout = ["no_indexOut"]

            outs = list(zip_longest(ads_out, ["out"], fillvalue="out"))

            outs = list(zip(outs, indexout))

            ## Values
            values_out = get.output_values(transaction)
            try:
                values_in = get.input_values(transaction)
            except:
                values_in = ["no_value"]

            # link input/output addresses to their bitcoin values
            rows_in = list(zip(ins, values_in))
            rows_out = list(zip(outs, values_out))

            #  Get the transaction id
            tx_id = get.txid(transaction)

            # Prepare data for a csv with 4 columns
            data = []
            data.append(rows_in)
            data.append(rows_out)
            data = list(chain.from_iterable(data))

            for g in data:
                # txid, tx_in_or_out, value, index_in_or_out, add_int
                res = [tx_id[0], g[0][0][1], g[1], g[0][1], g[0][0][0]]

                yield res


def nodes_links_using_dic(dic, dic_names=None, name_of_nodes=None):
    """Create relationships and nodes using a dictionary.

    Use the dictionary of addresses to create the addresses nodes
    and use the dictionary of clusters to create the relationships
    between the clusters and the addresses.

    Parameters
    ----------
    dic: dict
        The dictionary of addresses or the dictionary of the clusters
    dic_names: dict, optional
        The dictionary of the true identities
    name_of_nodes: str, optional
        The name of the nodes

    Yields
    ------
    List
        A list that will be written in a csv file, each
        element in the list will be written in a different column
        columns: nodes's ids.

   """
    from functions_extract_transform_data import GetInTransaction, force_rewrite

    ## If the dictionary of addresses is used, the addresses nodes are created
    if type(list(dic.keys())[1]) == str or type(list(dic.values())[1]) == str:

        print("Creating the addresses nodes and their properties...")
        # The keys of the dictionary must be the integers
        if type(list(dic.keys())[1]) == str:
            dic = dict(map(reversed, dic.items()))

        for k, v in dic.items():
            # add the actors true identity to the properties
            if dic_names:
                act_name = force_rewrite([k], dic_names)
            else:
                act_name = [0]

            # add a name for the nodes
            if name_of_nodes:
                # csv columns: addresse_id, name, btc_addresses, true_identity
                yield [k, name_of_nodes + str(k), v, act_name[0]]
            else:
                yield [k, v, act_name[0]]
    ## If the dictionary of clusters is used, links between
    # clusters and addresses are created
    else:
        print("Creating the relationships between the clusters and the addresses")
        dic = invert_dictionary(dic)
        for k, v in dic.items():
            if dic_names:
                act_name = force_rewrite([k], dic_names)
            else:
                act_name = [0]
            yield [k, act_name[0], v]


def create_nodes_actors(dict_clusters, name_of_node=None):
    """Create the actors nodes and their properties.

    Parameters
    ----------
    dict_clusters: dict
        The dictionary of the clusters
    name_of_node: str, optional
        The name of the nodes

    Yields
    ------
    list
        List containing the number of the clusters and the
        size of the clusters. The list will be written in a
        csv file with 2 or 3 columns : cluster_id, cluster_size
        plus the node name if specified.
   
   """
    for k, v in dict_clusters.items():
        if name_of_node:
            # columns : cluster id, name, cluster size
            yield [k, name_of_node + str(k), len(v)]
        else:
            yield [k, len(v)]
