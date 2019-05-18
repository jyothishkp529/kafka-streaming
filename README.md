# kafka-streaming
This is Kafka Streaming POC in Java.Version 8
## Objectives
1. Consume Transaction message data from IBM Mq
2. Handle failover scenario in case of cluster down.

## Cluster setup
#### Start ZooKeeper & Server
nohup bin/zookeeper-server-start.sh config/zookeeper.properties >  logs/zk.log &
nohup bin/kafka-server-start.sh config/server.properties > logs/server.log & 


#### Create Topic
bin/kafka-topics.sh --create --topic test --partitions 1  --replication-factor 1 --zookeeper localhost:2181 
bin/kafka-topics.sh --describe --topic test --zookeeper localhost:2181 

bin/kafka-topics.sh --create --topic sensorData --partitions 10  --replication-factor 1 --zookeeper localhost:2181 
bin/kafka-topics.sh --describe --topic sensorData --zookeeper localhost:2181 

bin/kafka-topics.sh --create --topic SupplierTopic --partitions 1  --replication-factor 1 --zookeeper localhost:2181 
bin/kafka-topics.sh --describe --topic SupplierTopic --zookeeper localhost:2181




bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic test

#### Verify Topic
bin/kafka-topics.sh --list --zookeeper localhost:2181


#### Sanity Test
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic transaction

###Producer Config
bootstrap.servers : list of servers:port. It is madatory. Atleast provide one.
key.serialiser
value.serialiser
partition.class

Reliability & Performance : These properties only at pProducer level
acks (0,1 and all) : 'all' is the slowest and most reliable option where as '0' being fastest unliable option.
retries
max.in.flights.requests.per.connection - very cruitial property

Asyn with call back
- It doesn't gurantee the order of delivery due to retry mechanism.

Other important Producer Config 
- buffer.memory
- compression.type
- batch.size
- lingr.ms
- client.id
- max.request.size

Note: Order of messages at a kafka partition can be maintained with the cost of throughput.


### Kafka Offsets
#### Offset types
-- Current offset : The point at which the consumer's recent read head points to. This helps broker not resent records same records.
-- commit offset : The offset position that the consumer that it is confirmed that it is processed. This lets consumer avoid duplicate processing of messages in case of partition rebalancing.

#### Commit
-- enable.auto.commit (default: true)
-- auto.commit.interval.ms ( default: 5 seconds)

#### manual commit pattern
-- Commit sync
-- Commit async

#### Rebalance Issues
When a rebalance occurs Kafka assigns a partition to a new consumer. This can lead to a situation when a messages gets processed again.
This can be addressed by defining a **RebalanceListener**.


##### RebalanceListener
It has two event methods.
- onPartitionRevoked
- onPartitionAssigned



