# Docker Compose Instructions

## Three different configurations
 - Redpanda
 - Kafka w/ Raft
 - Kafka w/ ZookKeeper



## RedPanda Instructions
1. run `docker-compose up` to start redpanda brokers
1. run `alias rpk="docker exec -ti redpanda-1 rpk"` to make commands easier
1. run `rpk topic create purchases -p 4` to make a topic with 4 partitions


### For Kafka with Zookeeper and KRaft
You must first create a custom network:
`docker create network my-network`


## Kafka with Zookeeper
1. run `docker-compose up` to start redpanda brokers
1. run `docker exec -ti kafka1 kafka-topics` to making running kafka-topics easier 
1. run `kafka-topics --bootstrap-server localhost:29092 --topic purchases2 --create --partitions 4 --replication-factor 1` to make a topic with 4 partitions
** It is required to add 20000 to any port you wish to connect to.  ex: 29092,29093,29094


## Kafka with KRaft 
1. run `docker-compose up` to start redpanda brokers
1. run `docker exec -ti kafka1 kafka-topics` to making running kafka-topics easier
1. run `kafka-topics --bootstrap-server localhost:9092 --topic purchases2 --create --partitions 4 --replication-factor 1` to make a topic with 4 partitions

