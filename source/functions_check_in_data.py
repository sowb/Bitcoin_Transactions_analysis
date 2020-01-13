"""List of the functions used to check the data.

A bunch of functions used to check if there is errors or anomalies
in the BTC data files, and among the addresses. Also functions to check 
the sizes of variables, and BTC data files; and functions to print 
items from a dictionary. 

This script is imported as module in other scripts. It must be stored
in the working directory.

"""

#  function to detect which file has an error
def detect_err(files: list, statistics=True) -> list:
    """Check if there is errors in the files. 
    
    This functions detect if there is an error such as unterminated 
    string in the JSON files. If any, it print the filename. This 
    function also count the number of lines in each file.
    
    Parameters
    ----------
    files: list
        A list of all files names
    statistics : bool, optional
        Print the statistics of the number of lines (default is True)
    
    Return
    ------
    list
        a list of the files lengths 
    print
        the prints if a file has an error or not
        print the statistic (if the parameter statistics = True
        
   """
    import json
    from scipy import stats

    lens = []
    for file in files:

        with open(file, "r") as f:
            try:

                for row, n in enumerate(f.readlines()):
                    line = json.loads(n)

                print(file, "is okay !!!")
                lens.append(row)
            except:
                print("Has ISSUES after row", row)

                continue

    if statistics == True:
        print(stats.describe(lens))
        return lens


# Detect all wrong addresses in the blockchain
def detect_wrong_adds(addresses):
    """This function check if an bitcoin address is a valid address
    
    Parameters
    ----------
    Addresses : list
        A list of addresses to be checked
    
    Returns
    -------
    list
        A list of bad invalid addresses
            
    """

    from hashlib import sha256

    digits58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

    def decode_base58(bc, length):
        """Source : http://rosettacode.org/wiki/Bitcoin/address_validation#Python"""
        n = 0
        for char in bc:
            n = n * 58 + digits58.index(char)
        return n.to_bytes(length, "big")

    def check_bc(bc):
        """Source : http://rosettacode.org/wiki/Bitcoin/address_validation#Python"""
        try:
            bcbytes = decode_base58(bc, 25)
            return bcbytes[-4:] == sha256(sha256(bcbytes[:-4]).digest()).digest()[:4]
        except Exception:
            return False

    anormal = []
    for adds in addresses:
        if check_bc(adds) == False:
            anormal.append(adds)
    return anormal


# compute the size of any file in the directory
def size_of_file(filenames: list) -> int:
    """Give a in Giga Bytes the size of one file or multiple files which 
	are in the directory 
    
    Parameters
    ----------
    filenames
        List of the filenames you want to know the size of. If you want 
        to know the size of one file: ['filename']
    
    Returns
    ------
    int
        Print the size of the file(s)
    
    """

    import os
    import sys

    try:
        s = 0
        for file in filenames:
            s += os.path.getsize(file)
        print("The size of", file, "is", round(s / 2 ** 30, 2), "GB")

    except:
        print(file, "not find in this directory", os.getcwd())


# retrieve addresses from a dictionary
def retrieve_addresses_name(dic: dict, add_int: int, in_dic: str):
    """Retrieve an address from the dictionary of addresses or from the clusters
    
    Parameters
    ----------
    dic
        The dictionary of addresses or clusters
    add_int
        Retrieve the address corresponding to this id
    in_dic: optional
        2 options : 'addresses' and 'clusters'
    
    Returns
    -------
    print: str, int
        Print a bitcoin address or the cluster id
        
    """
    from functions_extract_transform_data import invert_dictionary

    if in_dic == "addresses" or in_dic == "names":
        try:
            add_name = dic[add_int]
            return add_name
        except:
            print("The address corresponding to this key is:", add_name)

    elif in_dic == "clusters":
        d = invert_dictionary(dic, values=list)
        try:
            add_name = d[add_int]
            return add_name
        except:
            print("This actor belong to the cluster:", add_name)


# compute the size of a loaded file, or any variable
def size_of_variable(var):
    """Print the size of a variable defined in the global. 
    
    Parameters
    ----------
    var : any type
        variable name
        
    Returns
    ------
    int
        The size of the variable
    
    """

    import sys

    i = sys.getsizeof(var)
    if i < 1024:
        print(i, "bytes")
    elif i >= 1024 and i < 1048576:
        print("The size of", var, "is", round(i / 1024, 2), "KB")
    elif i >= 1048576 and i < 2 ** 30:
        print("The size of", var, "is", round(i / 1048576, 2), "MB")
    elif i >= 2 ** 30:
        print("The size of", var, "is", round(i / 2 ** 30, 2), "GB")
    else:
        print("Oops ! this variable is not defined")


# Print the keys and the values of a dictionary
def print_dict_items(Dic, p=None, q=None):
    """Print a list of dictionary items, keys and values. 
    
    Parameters
    ----------
    Dic: dict
        A dictionary
    p and q: int
        Start printing from p to q.
    
    Returns
    ------
    list
        list of pairs key-value from p to q
    
    """
    return dict(list(Dic.items())[p:q])
