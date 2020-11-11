bin/kafka-topics --create --topic UserRegions --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 
bin/kafka-topics --create --topic LargeRegions --zookeeper localhost:2181 --partitions 1 --replication-factor 1