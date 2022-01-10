# srds-project
-------------------------------
version with docker:
1)
create folders:
data-node-<index> for nodes
2)
run docker commands:
docker create network cassandra-network

seed:
docker run --name cassandra-node-<index> --network cassandra-network -v $(pwd)/data-node-<index>:/var/lib/cassandra -d  \
-p (9042+index):9042 --rm cassandra:latest
others nodes:
docker run --name cassandra-node-<index> -d --network cassandra-network -e CASSANDRA_SEEDS=cassandra-node-<seed-index>  \
-v $(pwd)/data-node-<index>:/var/lib/cassandra -d  -p (9042+index):9042 --rm cassandra:latest

3) install cqlsh:
pip install cqlsh
   
4) import schema:
cqlsh -f schema/schema.sql
   
5) verify cluster:
docker exec -it cassandra-node-0 nodetool status
   
example with 3 nodes:
seed:
docker run --name cassandra-node-0 --network cassandra-network -v $(pwd)/data-0:/var/lib/cassandra -d  \
-p 9042:9042 --rm cassandra:latest
other nodes:
docker run --name cassandra-node-1 -d --network cassandra-network -e CASSANDRA_SEEDS=cassandra-node-0  \
-v $(pwd)/data-1:/var/lib/cassandra -d  -p 9043:9042 --rm cassandra:latest

docker run --name cassandra-node-2 -d --network cassandra-network -e CASSANDRA_SEEDS=cassandra-node-0  \
-v $(pwd)/data-2:/var/lib/cassandra -d  -p 9044:9042 --rm cassandra:latest


