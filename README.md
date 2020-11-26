# Bitcoin Transactions Analysis
We studied the bitcoin transactions from 03/01/2009 to 09/09/2016.
Using neo4j, we did a clustering of the bitcoin addresses, and constructed the bitcoin network user graph (a graph database). 

## Prerequisites

### Software
- Python 3.7
- Neo4j Community Edition 3.5.6
- The machine we used had 64 RAM, 19 processors 

### Python Modules
- jupyter notebook
- numpy
- pandas
- py2neo

### Data to analyse
- Bitcoins Transactions : JSON format 
- The labels of the BTC Addresses (optional)

## The project Folder "Code_StageM2_BITUNAM"
Has 4 sub-folders : source, run, neo4j_scripts and results. 

### notebook 'Execute_the_scripts.ipynb' : 

- describes how to run the 4 scripts contained in the run folder
- does the clustering graph and export the clusters 
- explains how to construct the Bitcoin network graph using Neo4j.


### Folder source

Contains all the functions used for this project. 
- functions_check_in_data.py
- functions_write_save_load_data.py
- functions_extract_transform_data.py
- functions_for_the_clustering.py
- functions_for_bitcoin_graph

### Folder run 

Contains 4 scripts:
- create_data_clustering_h1.py : creates the dictionary of addresses, converts the bitcoin addresses to int, creates the data  
for the clustering graph with h1.
- add_heuristic_change_address.py : used to add the change addresses to the clustering data with h1.
- build_clusters.py : Used to create the dictionary of the clusters.
- create_data_bitcoin_graph.py : used to create the nodes and links for the bitcoin graph construction.

### Folder neo4j_scripts

Contains 3 Neo4j scripts, that must be executed on a SSH. 
- neo4j_script_clustering_graph.sh: used to buildthe clustering graph on Neo4j
- neo4j_script_export_partitions.sh: used to export partition to the working directory
- neo4j_script_bitcoin_graph.sh: used to build the Bitcoin Graph in Neo4j 

The folder contains also 3 scripts (.bat files) which can be used in a windows terminal.

### Folder results
Contains 4 Notebooks:
- create_dictionary_of_identities.ipynb: creates the dictionary of identities (labels)
- results_list_of_lists.ipynb: Results obtained with the list of lists of addresses
- results_clusters.ipynb: Statistics of the clusters
- neo4j_queries.ipynb: Some statistics obtained from the Bitcoin user graph.

  
   
    
     
      
       
        
         
         

  
  
  
  
  
  

##### Author:                                                                                                  
Boubacar SOW 

###### Contact:   
2s.boubacar@gmail.com

