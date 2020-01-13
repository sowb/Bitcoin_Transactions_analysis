# This script build the clustering graph. The nodes and the relationships are stored 
#in ~/Bitcoin_Transactions_analysis/

# Modify the neo4j.conf file, change the database name to "clustering_graph.db"
sudo nano /etc/neo4j/neo4j.conf

# if this error occurs: [ Error reading lock file /etc/neo4j/.neo4j.conf.swp: Not enough data read ] 
# The solution is to close the file without saving and execute the following line:
sudo rm /etc/neo4j/.neo4j.conf.swp

# If the "clustering_graph.db" is already running on a browser, it must be stopped
cd /var/lib/neo4j
sudo neo4j stop

# And must be deleted
cd /var/lib/neo4j/data/databases
sudo rm -r clustering_graph.db

# Construct the Clustering graph
cd /var/lib/neo4j
sudo neo4j-admin import --mode csv --database clustering_graph.db \
--nodes:Addresses ~/Bitcoin_Transactions_analysis/clustering_nodes.csv \
--ignore-duplicate-nodes=true \
--relationships:SAME_ACT ~/Bitcoin_Transactions_analysis/clustering_relationships.csv 

# IMPORTANT : Run the graph 
cd /var/lib/neo4j
sudo neo4j console

# And go back to the python notebook "excute_the_scripts.ipynb"


