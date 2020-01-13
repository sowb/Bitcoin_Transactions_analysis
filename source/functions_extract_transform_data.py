"""List of functions used to extract and transform the data.

This script contains the functions and the classes used to
extract informations from a transaction, from a container 
like a list, tuple, dict, set.

This script is imported as module in other scripts. The 
script must be stored in the working directory. 

"""

from functions_write_save_load_data import (
    dump_variable,
    load_row_by_row,
    load_dumped_data,
)

# Class to get infos in a transactions
class GetInTransaction:
    """Extract informations from a bitcoin transaction
    
    Informations such as the timestamp, the exchange rate, the 
    txid, the total_value, the input/output addresses, the 
    indexes, unique addresses. All the methods of the class has
    a transaction as a parameter. 
    
    Attributes
    ----------
    Empty
    
    Example:
    --------
        get =  GetInTransaction()
        get.input_addresses(row)
    """

    def timestamp(self, row):
        """Return the timestamp of the transaction in a list"""
        return [row["timestamp"]]

    def exchange_rate(self, row):
        """Return the exchange rate of the transaction"""
        return [row["exchange_rate"]]

    def input_addresses(self, row):
        """Get the input addresses in Tx"""
        return [row["tx_ins"][i]["address"] for i in range(len(row["tx_ins"]))]

    def index_in(self, row):
        """Get the indexes"""
        return [i for i in range(len(row["tx_ins"]))]

    def output_addresses(self, row):
        """Get the output addresses"""
        return [row["tx_outs"][i]["address"] for i in range(len(row["tx_outs"]))]

    def index_out(self, row):
        """Return a list of the index output addresses"""
        return [row["tx_outs"][i]["indexOut"] for i in range(len(row["tx_outs"]))]

    def input_values(self, row):
        """Return a list of the input values from the transaction"""
        return [row["tx_ins"][i]["value"] for i in range(len(row["tx_ins"]))]

    def output_values(self, row):
        """Return the output values from a transaction."""
        return [row["tx_outs"][i]["value"] for i in range(len(row["tx_outs"]))]

    def couples_ins_outs(self, row):
        """Return a couple of the number of addresses in input and the number of
        addresses in output from a transaction"""
        return len(row["tx_ins"]), len(row["tx_outs"])

    def txid(self, row):
        """Return the transaction id from a transaction"""
        return [row["txid"]]

    def total_value(self, row):
        """Return the total value from a transaction"""
        return [row["total_value"]]

    # collect unique addresses in each transaction
    def unique_addresses(self, row):
        """Returnn the unique addresses from a transaction"""
        all_unique_ads = set(self.input_addresses(row) + self.output_addresses(row))
        return list(all_unique_ads)

    def convert_time(self, row):
        """Convert the timestamp  of a transaction in a readable format."""
        from time import ctime

        return ctime(row["timestamp"])


class ExtractInfoListOfList:
    """A class used to retrieve informations from a list or list of lists.
        
    Attributes
    ----------
    LL: list
        A list or a list of lists
    
    """

    def __init__(self, LL):
        self.LL = LL

    def get_count(self, LL=False):
        """Count the elements of the list."""
        from collections import Counter
        from itertools import chain

        if LL is True:
            L = tuple(chain.from_iterable(self.LL))
        else:
            L = self.LL
        return Counter(L).most_common()

    def get_lengths(self, LL=True):
        """Return the lengths of the lists inside a list."""
        if LL is True:
            return [len(i) for i in self.LL]
        else:
            print("The data must be a list of lists")

    def get_lengths_count(self, LL=True, in_percentage=True, uniques=False):
        """Compute the lengths of a list and retrieve which has
        the most frequent len.
        
        Parameters
        ----------
        LL:bool, optional
            True if the parameter is a list of lists
        in_percentage: bool, optional
            Specifies if the counts should be computed in 
            percentage
        uniques:bool, optional
            Specifies if only unique elements should be 
            computed 
        
        Returns
        -------
        list
            A list of the result
        
        """
        from collections import Counter

        if LL is True:
            if uniques is False:

                l = [len(i) for i in self.LL]
            else:
                l = [len(set(i)) for i in self.LL]

            cnts = Counter(l).most_common()
            if in_percentage is True:
                res = [(t[0], (t[1] / len(l)) * 100) for t in cnts]
                return res
            else:
                return cnts
        else:
            print("The data must be a list of lists")


def addresses_to_dictionary(all_addresses, save_file, f_format):
    """Create the dictionary of addresses.
    
    Parameters
    ----------
    all_addresses: str
        filename containing the list of addresses
    save_file: str
        The name of the file where to save the dictionary
    
    Returns
    -------
    Save
        Write the dictionary on the hard drive
        
    """
    id_adds = set()
    for g in load_row_by_row(all_addresses):
        for i in g:
            id_adds.add(i)
    id_adds = dict(enumerate(id_adds))
    dump_variable(id_adds, save_file, f_format)


def invert_dictionary(dictionary, values_are=list):
    """Invert the dictionary of addresses or dictionary of clusters
    
    Parameters
    ----------
    dictionary: dict
        The dictionary to invert
    values_are: str, optional(default is list)
        If the option is 'list', the dictionary of clusters is used 
        
    Returns
    -------
    dict:
        Inverted dictionary
        
    """
    if values_are == list and type(dictionary[0]) != str:
        new_dic = dict((v, k) for k in dictionary for v in dictionary[k])
    else:
        new_dic = dict(map(reversed, dictionary.items()))
    return new_dic


def rewrite(line, dictionary):
    """Convert addresses to integers. 
    
    The keys of the dictionary used must be the same as the list's 
    elements to convert. The items in the list are replaced by the 
    values of the dictionary. 
    
    Parameters
    ----------
    line: tuple, list, set
        Addresses to convert container
    
    dictionary: dict
        Dictionary of addresses
    
    Returns
    ------- 
    list:
        List of numeric values. But also convert the 
        addresses in place
    
    Raises
    ------
    KeyError
        If the item in the list to convert is not a key of the
        dictionary
    
    """

    # if LL is a list of tuples
    if type(line) is not list and type(line) is not int:

        L = list(line)
        for ind, val in enumerate(L):
            L[ind] = dictionary[val]
        return L
    elif type(line) is int:
        L = [line]
        L[0] = dictionary[line]
        return L
    # if line is a list
    else:
        for ind, val in enumerate(line):
            line[ind] = dictionary[val]
        return line


def force_rewrite(col_to_convert, dictionary):
    """Convert a list of elements to the values of an dictionary.
    
    The items in the list are converted to the keys of the dictionary.
    If an item is not among the dictionary keys, the item is replaced
    by 'NA'. A new list is return. 
          
    Parameters
    ----------
    col_to_convert : list, pandas DataFrame column
        List of the items to convert
    dictionary: dict
        The dictionary used for the conversion
    
    Returns
    -------
    list
        List of the converted items
        
    """
    from itertools import chain

    res = []
    for i in col_to_convert:
        try:
            res.append(rewrite([i], dictionary))
        except KeyError as ke:
            res.append(["NA"])
            continue

    res = list(chain.from_iterable(res))
    return res
