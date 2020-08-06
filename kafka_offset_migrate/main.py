import argparse
import logging
import sys

from kafka_offset_migrate.kafka_client import KafkaClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def parse_command_line():
    parser = argparse.ArgumentParser(description='Simple tool to migrate '
                                                 'offsets between kafkas.')

    parser.add_argument('-s', '--src-kafka', default='localhost:9092',
                        help='The kafka address list from where get offsets.')

    parser.add_argument('-d', '--dst-kafka', default=None,
                        help='The kafka address list to where put offsets.')

    parser.add_argument('-i', '--src-zk', default='localhost:2181',
                        help='The zookeeper address list from where get '
                             'offsets.')

    parser.add_argument('-o', '--dst-zk', default=None,
                        help='The zookeeper address list to where put '
                             'offsets.')

    parser.add_argument('-t', '--topic', default=None,
                        help='Topic that owns the data.')

    parser.add_argument('-g', '--group', default=None,
                        help='Group that owns the offset.')

    parser.add_argument('-7', '--offset', default=500,
                        help='offset to set in 7o7')

    cmds = parser.add_subparsers(help='sub-command help')


    ## k2k
    kafka_to_kafka_cmd = cmds.add_parser('k2k',
                                         help='k2k will fetch offsets from '
                                              'src kafka and set then into dst'
                                              'kafka.')

    kafka_to_kafka_cmd.set_defaults(action='k2k')

    ## z2z
    zk_to_zk_cmd = cmds.add_parser('z2z',
                                   help='z2z will fetch offsets from '
                                        'src zookeeper and set then into dst'
                                        'zookeeper.')

    zk_to_zk_cmd.set_defaults(action='z2z')

    ## z2k
    zk_to_kafka_cmd = cmds.add_parser('z2k',
                                      help='z2k will fetch offsets from '
                                           'src zookeeper and set then into '
                                           'dst kafka.')

    zk_to_kafka_cmd.set_defaults(action='z2k')

    ## 7o7
    set_offset_cmd = cmds.add_parser('7o7',
                                      help='7o7 will set 500 as  offset for '
                                           'every partition in this consumer '
                                           'group.')
    set_offset_cmd.set_defaults(action='7o7')


    opts = parser.parse_args()

    if getattr(opts, 'dst_kafka', None) is None:
        logger.error("Missing option dst kafka (-d)")
        parser.print_help()
        sys.exit(1)

    if getattr(opts, 'action', None) is None:
        logger.error("Missing operation. You must choose one of "
                     "{z2z, k2k, z2k, 7o7}")
        parser.print_help()
        sys.exit(1)

    print(opts)

    return opts


def main():
    logging.basicConfig(
        format='%(asctime)s: %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    opts = parse_command_line()

    src_kafka_hosts = getattr(opts, 'src_kafka', None)
    dst_kafka_hosts = getattr(opts, 'dst_kafka', None)
    src_zk_hosts = getattr(opts, 'src_zk', None)
    dst_zk_hosts = getattr(opts, 'dst_zk', None)
    group_id = getattr(opts, 'group', None)
    topic = getattr(opts, 'topic', None)
    offset = int(getattr(opts, 'offset', 500))

    client = KafkaClient(group_id, topic, src_kafka_hosts, dst_kafka_hosts,
                     src_zk_hosts, dst_zk_hosts)
    if opts.action == 'k2k':
        client.kafka_to_kafka()
    elif opts.action == 'z2k':
        client.zk_to_kafka()
    elif opts.action == 'z2z':
        client.zk_to_zk()
    elif opts.action == '7o7':
        client.set_offset_value(offset)
    else:
        raise NotImplementedError
