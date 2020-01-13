"""List of functions to save and load data. 

This script contains all the functions used to load data 
files from the working directory, and contains functions 
used to save the transformed data.

This script is imported as module in other scripts. The 
script must be stored in the working directory.

"""
# get all original btc files in memory
def get_files():
    """Get file names starting with 'part' from the hard drive. 
    
    Parameters
    ----------
    None
    
    Return
    ------
     List
         list of names
    
    """
    from glob import glob

    all_files = glob("part*")
    all_files.sort()
    return all_files


# load data files, apply a specific function and save the results into a new file
def load_apply_save(load_files, func, save_file):
    """Charge data apply a function and save the result.
    
    Charge the BTC data (JSON files), apply a specific func, 
    write the result in a file and save it on the hard drive. 
    
    Parameters
    ----------
    load_files: list
        List of the data files to load. The files must be in JSON 
    func: function
        A function to apply to the data
    save_file: str
        The name of the file in which the data will be written
        
    Returns
    -------
    Write the result in 'save_file'
    
    """
    import json

    z = 0
    i = 0
    with open(save_file, "w") as f:
        for file in load_files:
            z += 1
            if z % 1 == 0:
                print("file", z)

            with open(file, "r") as in_json:
                for n, row in enumerate(in_json.readlines()):

                    i += 1
                    if i % 10000000 == 0:
                        print(i)
                    try:
                        line = json.loads(row)
                        f.write("%s\n" % func(line))
                    except NameError as error1:
                        print(error1)
                        break
                    except:
                        print("Error at line", n)
                        continue


# Reload a saved file row by row and apply a specific function a save
def reload_apply_save(
    load_file, save_file, func, func_param=None, pattern_to_remove=None
):
    """Load a dumped data, apply a function and save it.
    
    Load the list of lists of bitcoin addresses, convert
    the addresses from string to integers, and save the result.
        
    Parameters
    ----------
    load_file : str
        Name of the file containing the list of lists to load
    save_file : str
        save the result in this file
    func: function
        Function used to convert the BTC addresses to integers
    func_param: variable, str
        A parameter of func
    pattern_to_remove: str
        Description of the pattern to remove in the list (or data)
    
    Returns
    -------
    write : list
        a list of the converted addresses in int 
    
    Example
    -------
    >>> reload_apply_save('input_addresses_str', 'input_addresses_int',
                            rewrite, addresses_ids, ' ')
    [20, 1, 2]
    The result is written in the file 'input_addresses_int', for each
    transaction.

    """
    from ast import literal_eval as make_list

    with open(save_file, "w") as save:

        with open(load_file, "r") as f:
            i = 0
            for line in f:
                i += 1
                if i % 10000000 == 0:
                    print(i, "Transactions processed...")
                row = make_list(line.strip())

                # Remove the pattern specified
                if pattern_to_remove is not None and pattern_to_remove in row:
                    row.remove(pattern_to_remove)

                elif func_param is None:
                    save.write("%s\n" % func(row))
                else:
                    save.write("%s\n" % func(row, func_param))
            print("The total number of transaction treated is:", i)


# Reload data row by row in memory as a generator
def load_row_by_row(filename):
    """Load a data file row by row.
    
    Parameters
    ----------
    filename: str 
        The file containing the data to load
        
    Yields:
    --------
    list:
      A line of the file to load.
      
    """
    from ast import literal_eval as make_list

    with open(filename, "r") as f:
        i = 0
        for n, line in enumerate(f):

            i += 1
            if i % 10000000 == 0:
                print(i, "lines processed")
            try:
                row = make_list(line.strip())
            except:
                print("Error at line", n)
                continue
            yield row


# Save a variable in hard drive in pickle or json format
def dump_variable(variable, save_file, in_format):
    """Write a variable on the hard drive.
    
    Parameters
    ----------
    variable: any type (list, tuple, dict...)
        The variable containing the data to dump
    save_file: str
        The name of the file in which to save.
    in_format: str, optional
        Specifies in which format to save the data.
        The options are : JSON, pickle, hdf5, npy
        
    Returns
    -------
    write 
        Write the data in a new file on hard drive
        
    """
    import h5py
    import pickle
    import numpy as np

    # dump in pickle
    if in_format == "pickle":
        with open(save_file, "wb") as out_pickle:
            pickle.dump(variable, out_pickle)

    # dump in JSON
    elif in_format == "JSON":
        with open(save_file, "w") as out_json:
            json.dump(variable, out_json)

    # dump in hdf5
    elif in_format == "hdf5":
        to_dump = np.array(variable)
        #  if the data to dump is a list
        if to_dump.dtype == np.dtype("int64") or to_dump.dtype == np.dtype("int32"):

            with h5py.File(save_file, "w") as f:
                dset = f.create_dataset("data", data=to_dump, dtype="i")

        # if the data to dump is list lists
        else:
            dt = h5py.special_dtype(vlen=str)
            with h5py.File(save_file, "w") as f:
                dset = f.create_dataset("data", data=to_dump, dtype=dt)
    #   dump the variable in numpy:
    elif in_format == "npy":
        np.save(save_file, variable)

    else:
        print("Wrong type. in_format should be 'pickle', 'hdf5', 'npy' or 'JSON'")


# load a dumped variable by dump_variable() :
def load_dumped_data(load_file, file_format):
    """Load the data dumped by the function 'dump_variable'. 
    
    Load the data dumped in pickle, JSON, npy and hdf5 formats.
    
    Parameters
    ----------
    load_file: str, filename
        The filename to load from hard drive.
    file_format: str, optional
        The format in which to load the data. 
        4 options : JSON, pickle, hdf5, npy.
    
    Returns
    -------
        The file to load. 
        
    """
    import pickle
    import h5py
    import numpy as np
    from ast import literal_eval

    if file_format == "pickle":
        file = pickle.load(open(load_file, "rb"))
        return file

    elif file_format == "hdf5":
        open_file = h5py.File(load_file, "r")
        array_data = open_file["data"][:]
        list_data = array_data.tolist()
        # if the data loaded is a list
        if array_data.dtype == np.dtype("object"):
            print("the items in this list are strings")
            #   list_data = [literal_eval(l) for l in list_data]
            list_data = list(map(lambda x: json.loads(x), list_data))
            return list_data
        else:
            return list_data

    elif file_format == "json":
        try:
            with open(load_file) as in_json:
                for row in in_json.readlines():
                    file = json.loads(row)
                    return file
        except:
            print("Can not load data. %load_file must be stored with json.dumb()")

    elif file_format == "npy":
        file = np.load(load_file, allow_pickle=True)
        return file


def write_to_csv(out_file, write_or_append, func_generate, header=[]):
    """Write the content of a row to a csv file object. 
    
    The row to write must come from a generator. Each element
    from the row will be written in a column. The csv delimiter
    is ','.
    
    Parameters
    ----------
    out_file: str
        The name of the file
    write_or_append: str, optional
        'w' for writing to the file,
        'a' for appending to the file
    func_generate: function
        The function which generate the row
    header: str
        The first row of the csv file (the header)
        If empty, no header will be written
    
    Returns
    -------
        Write the row to out_file
        
    """
    import csv
    import os

    with open(out_file, write_or_append, newline="") as f:

        writer = csv.writer(f, delimiter=",")
        if len(header) != 0:
            writer.writerow(header)
        cnt = 0
        for n in func_generate:
            writer.writerow(list(n))
            cnt = cnt + 1
    print(cnt, "rows and", len(header), "columns created in", out_file)


def run_terminal_script(file_to_run):
    """Run a windows script
    
    Might also work on linux Os. 
    
    Parameters
    ----------
    file_to_run: str
        The file containing the code to run
    
    Returns
    -------
         A CompletedProcess instance
         
    """

    import subprocess

    subprocess.run(
        [file_to_run],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )


def write_table_in_latex(df, save_in_file, title):
    """Convert a pandas DataFrame to a latex table and
    write it to a file.
    
    Parameters
    ----------
    save_in_file: str
        The name of the file where to write
        
    title: str
        The title to write before the table
        
    Returns
    -------
        Write latex code to a file
        
    """
    import pandas as pd

    with open(save_in_file, "a") as f:
        f.write(title + "\n")
        f.write(df.to_latex(index=False))
