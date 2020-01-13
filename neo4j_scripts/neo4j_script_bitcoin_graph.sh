#This script build the Bitcoin Graph. The nodes and the relationships are stored 
#in ~/Bitcoin_Transactions_analysis/. 

# Modify the neo4j.conf file, change the database name to "bitcoin_user_graph.db"
sudo nano /etc/neo4j/neo4j.conf

# If the "bitcoin_user_graph.db" is already running on a browser, it must be stopped
cd /var/lib/neo4j
sudo noe4j stop

# And must be deleted
cd /var/lib/neo4j/data/databases
sudo rm -r bitcoin_user_graph.db

# Construct the Bitcoin graph
cd /var/lib/neo4j
sudo neo4j-admin import --mode csv --database bitcoin_user_graph.db \
--nodes:Addresses ~/Bitcoin_Transactions_analysis/nodes_addresses.csv \
--nodes:Actors ~/Bitcoin_Transactions_analysis/nodes_actors.csv \
--nodes:Transactions ~/Bitcoin_Transactions_analysis/nodes_transactions_data.csv \
--ignore-duplicate-nodes=true \
--relationships:IS_IN_TRANSACTION ~/Bitcoin_Transactions_analysis/relationships_transactions_addresses.csv \
--relationships:BELONG_TO_ACTOR ~/Bitcoin_Transactions_analysis/relationships_addressesids_actors.csv 

# Get the recommandations for the memory settings in the neo4j.conf
sudo neo4j-admin memrec --database bitcoin_user_graph.db

# Change the following lines in the neo4j.conf
dbms.memory.heap.initial_size=24100m
dbms.memory.heap.max_size=24100m
dbms.memory.pagecache.size=28100m

# Run the graph on a browser 
sudo neo4j console

