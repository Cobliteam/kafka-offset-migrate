#!/usr/bin/env python

from kazoo.client import KazooClient
import kafka
import logging
from kafka.errors import OffsetOutOfRangeError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class KafkaClient(object):
    def __init__(self, group_id, topic, src_kafka_hosts, dst_kafka_hosts,
                 src_zk_hosts, dst_zk_hosts):
        self.group_id = group_id
        self.topic = topic
        self.src_kafka_hosts = src_kafka_hosts
        self.dst_kafka_hosts = dst_kafka_hosts
        self.src_zk_hosts = src_zk_hosts
        self.path = "/consumers/{}/offsets/{}".format(self.group_id,
                                                      self.topic)

    def get_offsets_from_zk(self, path, zk_hosts):
        zk = KazooClient(hosts=zk_hosts, read_only=True)
        zk.start()

        (rawOffsets, stats) = zk.get(path)

        zk.stop()
        zk.close()

        offsets = {}

        for rawOffset in rawOffsets.decode('utf-8').split(','):
            [partition, offset] = rawOffset.split(':')
            offsets[partition] = int(offset)

        # print(sorted(offsets.items(), key=lambda t : int(t[0])))

        return offsets

    def convert_offsets_to_timestamp(self, topic, kafka_offsets, kafka_hosts):
        consumer = kafka.KafkaConsumer(
            bootstrap_servers=kafka_hosts,
            group_id=None,
            enable_auto_commit="false",
            auto_offset_reset="none",
            consumer_timeout_ms=1000
        )

        timestamps = {}

        for tp, offset in kafka_offsets.items():
            consumer.assign([tp])
            if not offset is None:
                consumer.seek(tp, max(int(offset) - 1, 0))
                try:
                    msg = next(consumer)
                except (StopIteration, OffsetOutOfRangeError) as err:
                    print("Message does not exist: {}".format(err))
                else:
                    timestamps[tp] = msg.timestamp
            else:
                msg = "Partition: {} has ans invalid offset: {}"
                logger.debug(msg.format(tp, offset))


        consumer.close()

        return timestamps

    def convert_timestamp_to_offsets(self, group_id, topic, timestamps,
                                     kafka_hosts):
        consumer = kafka.KafkaConsumer(
            bootstrap_servers=kafka_hosts,
            group_id=None,
            enable_auto_commit="false",
            auto_offset_reset="none",
            consumer_timeout_ms=1000
        )

        offsets = consumer.offsets_for_times(timestamps)

        consumer.close()

        return offsets

    def zk_offsets_to_kafka_offsets(self, zk_offsets, topic):
        kafka_offsets = {}
        for partition, offset in zk_offsets.items():
            tp = kafka.TopicPartition(topic, int(partition))
            kafka_offsets[tp] = offset

        return kafka_offsets

    def kafka_offsets_to_zk_offsets(self, kafka_offsets):
        zk_offsets = {}

        for entry in kafka_offsets.items():
            zk_offsets[str(entry[0].partition)] = entry[1].offset

        return zk_offsets

    def set_offsets_into_zk(self, zk_offsets, path, zk_hosts):
        zk = KazooClient(hosts=zk_hosts)
        zk.start()
        data_str = ",".join(map(
            lambda entry: "{}:{}".format(entry[0], entry[1]),
            zk_offsets.items()
        ))
        data = data_str.encode()

        zk.ensure_path(path)
        zk.set(path, data)

        zk.stop()
        zk.close()

    def get_offsets_from_kafka(self, group_id, topic, kafka_hosts):
        consumer = kafka.KafkaConsumer(
            bootstrap_servers=kafka_hosts,
            group_id=group_id,
            enable_auto_commit=False
        )

        consumer.topics()
        partitions = consumer.partitions_for_topic(topic)
        topic_partition = map(
            lambda p: kafka.TopicPartition(topic, p),
            partitions)

        kafka_offsets = {tp: consumer.committed(tp) for tp in topic_partition}

        consumer.close()

        return kafka_offsets

    def set_offsets_into_kafka(self, kafka_offsets, group_id, topic,
                               kafka_hosts):
        consumer = kafka.KafkaConsumer(
            bootstrap_servers=kafka_hosts,
            group_id=group_id,
            enable_auto_commit=False
        )

        offset_and_metadata = {
            tp: kafka.OffsetAndMetadata(offset.offset, b'') for
            tp, offset in kafka_offsets.items()}

        print(offset_and_metadata)

        consumer.topics()
        consumer.commit(offset_and_metadata)
        consumer.close()

    def kafka_to_kakfa(self):
        # Get offsets from src kafka
        old_kafka_offsets = self.get_offsets_from_kafka(self.group_id,
                                                        self.topic,
                                                        self.src_kafka_hosts)
        # Convert Offsets to timestamp
        timestamps = self.convert_offsets_to_timestamp(self.topic,
            old_kafka_offsets, self.src_kafka_hosts)
        # Get offsets from dst kafka
        new_kafka_offsets = self.convert_timestamp_to_offsets(self.group_id,
            self.topic, timestamps, self.dst_kafka_hosts)
        # Load offsets into dst kafka
        self.set_offsets_into_kafka(new_kafka_offsets, self.group_id,
                                    self.topic, self.dst_kafka_hosts)

    def zk_to_zk(self):
        # Get offsets from src zookeeper
        old_zk_offsets = self.get_offsets_from_zk(self.path, self.src_zk_hosts)
        # Convert Offsets to timestamp
        old_kafka_offsets = self.zk_offsets_to_kafka_offsets(old_zk_offsets,
                                                             self.topic)
        timestamps = self.convert_offsets_to_timestamp(self.topic,
                                                       old_kafka_offsets,
                                                       self.src_kafka_hosts)
        # Get offsets from dst kafka
        new_kafka_offsets = self.convert_timestamp_to_offsets(self.group_id,
            self.topic, timestamps, self.dst_kafka_hosts)
        new_zk_offsets = self.kafka_offsets_to_zk_offsets(new_kafka_offsets)
        # Load offsets into dst zookeeper
        self.set_offsets_into_zk(new_zk_offsets, self.path, self.dst_zk_hosts)

    def zk_to_kafka(self):
        # Get offsets from src zookeeper
        old_zk_offsets = self.get_offsets_from_zk(self.path, self.src_zk_hosts)
        # Convert Offsets to timestamp
        old_kafka_offsets = self.zk_offsets_to_kafka_offsets(old_zk_offsets,
                                                             self.topic)
        if False:
            timestamps = self.convert_offsets_to_timestamp(
                self.topic, old_kafka_offsets, self.src_kafka_hosts)
        # Get offsets from dst kafka
            new_kafka_offsets = self.convert_timestamp_to_offsets(
                self.group_id, self.topic, timestamps, self.dst_kafka_hosts)
        else:
            new_kafka_offsets = old_kafka_offsets
        # Load offsets into dst kafka
        self.set_offsets_into_kafka(new_kafka_offsets, self.group_id,
                                    self.topic, self.dst_kafka_hosts)

