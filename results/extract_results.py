"""Contain fonctions to extract/show some results. 
"""
def biggest_clusters(clusters):
    """Return the clusters from the biggest to the lowest."""
    import pandas as pd
    from functions_extract_transform_data import ExtractInfoListOfList

    ll = list(clusters.values())
    ex = ExtractInfoListOfList(ll)
    data = ex.get_lengths()
    df = pd.DataFrame(
        list(enumerate((data))), columns=["Cluster_Id", "size(nb of addresses)"]
    )
    return df

def nb_addresses_freq_in_clusters(data):
    """Compute the size of the most frequent clusters.
    
    Paramaters
    ----------
    data: dict, list
        Dictionary of the clusters or list
    
    Returns:
    DataFrame of most frequent clusters
    
    """
    import pandas as pd
    from functions_extract_transform_data import ExtractInfoListOfList
    if type(data)==dict:
        ll = list(data.values())
    else:
        ll = data
    ex = ExtractInfoListOfList(ll)
    data = ex.get_lengths_count()

    df = pd.DataFrame(data, columns=["nb_ads", "counts"])
    percentage = [((t[1] / len(ll)) * 100) for t in data]
    df["%p"] = percentage
    return df

def write_in_list(txt, n):
    """Return a list of strings."""
    lst = []
    for i in range(n):
        lst.append(txt + str(i))
    print(lst)
    

def execute(load_npfile, save_top10, f_format):
    
    """Load the lists of lists input or output addresses, 
    and count the number of addresses.
    
    Parameters
    ----------
    load_npfile: numpy array
        List of lists converted to numpy array
    save_top10: str, filename
        Save the top 10, most frequent addresses
    
    f_format : str, optional
        save in 4 formats : JSON, pickle, npy, hdf5
        
    Returns
    -------
    Write 
    
    """
    import numpy as np
    from functions_extract_transform_data import ExtractInfoListOfList
    from functions_write_save_load_data import dump_variable
    from itertools import chain
    
    adds = np.load(load_npfile, allow_pickle= True)
    adds = adds.tolist()
    adds = list(chain.from_iterable(adds))

    ext = ExtractInfoListOfList(adds)
    count_adds = ext.get_count(LL=False)

    to_df = count_adds[0:10]
    dump_variable(to_df, save_top10, f_format)
