::runas /noprofile \user:LEGION
:: Copy nodes and relationships csv files to the neo4j folder
robocopy C:\Users\BouBa\Bitcoin_Transactions_analysis C:\Users\BouBa\Desktop\neo4j-community-3.5.6\import\ ^
nodes_actors.csv nodes_addresses.csv nodes_transactions_data.csv relationships_addressesids_actors.csv relationships_transactions_addresses.csv

:: Change the working directory and stop neo4j if it is running
cd C:\Users\BouBa\Desktop\neo4j-community-3.5.6\bin
START neo4j stop 

:: If there is another existing database with the same name, it must be deleted first
RMDIR /Q/S C:\Users\BouBa\Desktop\neo4j-community-3.5.6\data\databases\bitcoin_graph.db

:: CHANGE the database name : onpen neo4j.conf file and change the name
cd C:\Users\BouBa\Desktop\neo4j-community-3.5.6\conf
neo4j.conf

:: Now construct the graph by running :
cd C:\Users\BouBa\Desktop\neo4j-community-3.5.6\bin
START neo4j-admin import --mode csv --database bitcoin_graph.db ^
--nodes:Addresses C:\Users\BouBa\Desktop\neo4j-community-3.5.6\import\nodes_addresses.csv ^
--nodes:Actors C:\Users\BouBa\Desktop\neo4j-community-3.5.6\import\nodes_actors.csv ^
--nodes:Transactions C:\Users\BouBa\Desktop\neo4j-community-3.5.6\import\nodes_transactions_data.csv ^
--ignore-duplicate-nodes=true ^
--relationships:BELONG_TO_ACTOR C:\Users\BouBa\Desktop\neo4j-community-3.5.6\import\relationships_addressesids_actors.csv ^
--relationships:IS_IN_TRANSACTION C:\Users\BouBa\Desktop\neo4j-community-3.5.6\import\relationships_transactions_addresses.csv
 
:: update-service and run the database online
::START runas /noprofile \user:LEGION neo4j restart 
::START neo4j start 
