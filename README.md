# kafka-offset-migrate
This is a simple tool to migrate consumer offtsets from a kafka cluster to another.

In order to migrate a consumer group GROUP of a topic TOPIC you must stop any client
consuming in this group and run:
```bash
$ kafka-offset-migrate k2k -s $SOURCE_KAFKA -d $DESTINATION_KAFKA -g $GROUP -t $TOPIC
```

## Usage
```bash
$ kafka-offset-migrate -h

usage: kafka-offset-migrate [-h] [-s SRC_KAFKA] [-d DST_KAFKA] [-i SRC_ZK]
                            [-o DST_ZK] [-t TOPIC] [-g GROUP]
                            {k2k,z2z,z2k} ...

Simple tool to migrate offsets between kafkas.

positional arguments:
  {k2k,z2z,z2k}         sub-command help
    k2k                 k2k will fetch offsetsi from src kafka and set then
                        into dstkafka.
    z2z                 z2z will fetch offsets from src zookeeper and set then
                        into dstzookeeper.
    z2k                 z2k will fetch offsets from src zookeeper and set then
                        into dst kafka.

optional arguments:
  -h, --help            show this help message and exit
  -s SRC_KAFKA, --src-kafka SRC_KAFKA
                        The kafka address list from where get offsets.
  -d DST_KAFKA, --dst-kafka DST_KAFKA
                        The kafka address list to where put offsets.
  -i SRC_ZK, --src-zk SRC_ZK
                        The zookeeper address list from where get offsets.
  -o DST_ZK, --dst-zk DST_ZK
                        The zookeeper address list to where put offsets.
  -t TOPIC, --topic TOPIC
                        Topic that owns the data.
  -g GROUP, --group GROUP
                        Group that owns the offset.
```
