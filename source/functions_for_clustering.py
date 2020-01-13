"""List of functions used to do the address clustering. 

This script contains the functions to use to create the data for 
the clustering of the BTC addresses. All the functions write the
resulting data in a file on the hard drive. The nodes and the relationships
for the clustering graph are written in csv files.

This script is imported as module in other scripts. The script must 
be stored in the working directory. 

"""
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


def generate_nodes(filename):
    """Create nodes data for a graph.
    
    Prepare the data to be written in a csv file. The function
    generate the data for each column in the csv.
    
    Parameters
    ----------
    filename: str
        The data to use for the nodes.
        The data file must contain a list of lists.
    
    Yields
    ------
    list
        A list containing to values, the node id and the node name
    
    """
    n_ids = set()

    for i in load_row_by_row(filename):
        if len(i) >= 2:
            ads = set(i)
            for ad in ads:
                n_ids.add(ad)

    nodes = zip(n_ids, n_ids)
    for node in nodes:
        yield node


def generate_rels(filename):
    """Create relationships data for a graph.
    
    Prepare a list of lists of integers to be written as graph links
    in a csv file. 
    
    Parameters
    ---------
    filename: str
        The data to use for the relationships.
        The data file must contain a list of lists.
    
    Yields
    ------
    list
        A link between two nodes.
    
    """
    all_links = set()

    for i in load_row_by_row(filename):
        if len(i) >= 2:
            ads = list(set(i))
            for ad in range(len(ads)):
                try:
                    link = (ads[0], ads[ad + 1])
                    all_links.add(link)
                except:
                    continue

    for link in all_links:
        yield link


def build_save_clusters(load_file, save_file, f_format):
    """Construct the clusters using the partitions extracted from neo4j.
    
    
    Parameters
    ----------
    load_file: str
        The file containing the neo4j partitions
    save_file: str
        The Filename in which to save the data
    f_format: str, optional
        Precise the format in which to save the data
        3 options : JSON, pickle, npy, hfd5
        
    Returns
    -------
    Save 
        Save the clusters in a file
       
    """
    import csv

    clusters = {}
    with open(load_file) as f:
        reader = csv.reader(f)
        next(reader)

        for r in reader:
            if str(r[0]) in clusters:
                clusters[str(r[0])].append(int(r[1]))
            else:
                clusters[str(r[0])] = [int(r[1])]
    # rename :
    list_clst = [val for key, val in clusters.items()]
    # Sort the clusters by desc, biggest clusters start from 0
    list_clst = sorted(list_clst, key=lambda x: len(x), reverse=True)
    # Transform the clusters into a dictionary
    clusters = dict(enumerate(list_clst))
    # save the clusters
    dump_variable(clusters, save_file, f_format)


def append_rest_of_clusters(
    load_adds, load_adds_type, load_clusters_file, save_file, save_in_format
):
    """Add the rest of the addresses to the clusters.
    
    The dictionary of addresses can be used to add the rest
    of addresses to the clusters. Input an output addresses 
    can also be used. 
        
    Parameters
    ----------
    load_adds: str
        filename to load containing the list of addresses in 
        input or output
    load_adds_type: str, optional
        'list' if the addresses to append are in a list
        'dict' if the addresses to append are in a dictionary
    load_clusters_file: str
        The file containing the clusters in which to append
    save_file:str
        The filename in which to save the result
    save_in_format:str, optional
        4 options: JSON, pickle, npy, hdf5
    
    Returns
    -------
    Write
        Save the result in a new file.
        
 """
    from itertools import chain

    if load_adds_type == list:
        actor = set()
        for i in load_row_by_row(load_adds):
            if len(i) < 2:
                actor.add(i[0])

    elif load_adds_type == dict:
        actor = load_dumped_data(load_adds, "pickle")
        actor = [i for i in range(len(actor))]

    clusters = load_dumped_data(load_clusters_file, "pickle")

    print("Starting point :", len(clusters), "clusters")

    C = invert_dictionary(clusters, list)

    for a in actor:
        if a not in C:
            # clusters[len(clusters)]=[a]
            clusters.update({len(clusters): [a]})

    print("The Total number of the clusters is :", len(clusters))

    dump_variable(clusters, save_file, save_in_format)


# clustering data by heuristic 2:
def change_address(files, save_file, addresses_ids):
    """Get the change address in a output BTC transaction.
    
    The change address is selected according to the heuristic
    proposed by Athey et al.(2017). In a two-output transaction, 
    if one of the outputs has 3 more decimal places than other
    output value (which has 4 or fewer decimal places), we declare
    the output with the larger number of decimal digits to be the
    change address.
    
    Parameters
    ----------
    files: list
        List of the data files names
    save_file: str
        Filename in which to save the result
    addresses_ids: dict
        Dictionary of addresses in which the addresses are keys
    
    Returns
    -------
    write rows 
        A row is a list containing the change address and 1 input address
        
    """

    import os
    from itertools import chain

    # 1btc = 10**8 satoshi
    if os.path.isfile(save_file):
        os.remove(save_file)

    get = GetInTransaction()
    cnt = 0
    with open(save_file, "w") as save:
        for file in files:
            for tx in load_row_by_row(file):
                cnt += 1
                if cnt % 1000000 == 0:
                    print(cnt, "transactions processed...")
                # if it is a transaction with two outputs, select values and compute the number of decimals of each value
                if len(get.output_values(tx)) == 2:

                    output_addresses = get.output_addresses(tx)
                    values = get.output_values(tx)

                    decimals_indexes = []
                    for ind, j in enumerate(values):
                        for i in range(9):
                            if j % 10 ** i != 0:
                                decimals_indexes.append(zip([9 - i], [ind]))
                                break
                        else:
                            decimals_indexes.append(zip([0], [ind]))

                    # Select input/output addresses
                    decimals_indexes = [list(i) for i in decimals_indexes]
                    decimals_indexes = list(chain.from_iterable(decimals_indexes))
                    #  conditions to repect for h2 clustering
                    if (
                        decimals_indexes[0][0] <= 4 or decimals_indexes[1][0] <= 4
                    ) and abs(decimals_indexes[0][0] - decimals_indexes[1][0]) > 3:
                        # the change addresse is the addresse with the highest decimal
                        if decimals_indexes[0][0] < decimals_indexes[1][0]:
                            change_addresse = output_addresses[decimals_indexes[1][1]]
                        elif decimals_indexes[0][0] > decimals_indexes[1][0]:
                            change_addresse = output_addresses[decimals_indexes[0][1]]

                        # collect actor addresse's : rassemble input and change addresses
                        same_actor = get.input_addresses(tx)[:1] + [change_addresse]

                        same_actor = list(set(same_actor))
                        # convert addresses in strings to intergers
                        rewrite(same_actor, addresses_ids)

                        # save the list
                        save.write("%s\n" % same_actor)
                else:
                    continue
