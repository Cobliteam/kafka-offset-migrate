# kafka-offset-migrate
This is a simple tool to migrate consumer offtsets from a kafka cluster to a another.

In order to migrate a consumer group GROUP of a topic TOPIC you must stop any client
consuming in this group and run:
```bash
$ kafka-offset-migrate k-2-k -s $SOURCE_KAFKA -d $DESTINATION_KAFKA -g $GROUP -t $TOPIC
```
