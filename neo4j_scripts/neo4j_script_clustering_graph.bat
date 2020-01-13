::Copy the necessary files
START robocopyC:\Users\BouBa\Bitcoin_Transactions_analysis C:\Users\BouBa\Desktop\neo4j-community-3.5.6\import\ ^
clustering_nodes.csv clustering_relationships.csv
SLEEP 10


:: Change the working directory and stop neo4j if it is running
cd C:\Users\BouBa\Desktop\neo4j-community-3.5.6\bin
START neo4j stop
SLEEP 30

::If needed, remove the existent database
RMDIR /Q/S C:\Users\BouBa\Desktop\neo4j-community-3.5.6\data\databases\clustering_graph.db


::open the conf file and change the database name if needed
cd C:\Users\BouBa\Desktop\neo4j-community-3.5.6\conf
neo4j.conf

::Do The CLUSTERING GRAPH
::Change the Directory
cd C:\Users\BouBa\Desktop\neo4j-community-3.5.6\bin
START neo4j-admin import --mode csv --database clustering_graph.db ^
--nodes:Addresses C:\Users\BouBa\Desktop\neo4j-community-3.5.6\import\clustering_nodes.csv ^
--ignore-duplicate-nodes=true ^
--relationships:SAME_ACT C:\Users\BouBa\Desktop\neo4j-community-3.5.6\import\clustering_relationships.csv 





