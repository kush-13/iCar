import sys
import os

from confluent_kafka import Consumer, KafkaException, KafkaError

if __name__ == '__main__':
    topics = ['wurdvdfa-testing']

    conf = {
        'bootstrap.servers': 'omnibus-01.srvs.cloudkafka.com:9094,omnibus-02.srvs.cloudkafka.com:9094,omnibus-03.srvs.cloudkafka.com:9094',
        'session.timeout.ms': 6000,
        'group.id': 'group1',
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': 'wurdvdfa',
        'sasl.password': 'tXchfG1MeEi6LBiEX4dZtBTEaPJtDgHc'
    }

    c = Consumer(**conf)
    c.subscribe(topics)
    try:
        while True:
            msg = c.poll(timeout=0.2)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    print(KafkaException(msg.error()))
            else:
                # Motor code to be added here
                print(msg.value())

    except KeyboardInterrupt:
        print('Aborted by user\n')

    # Close down consumer to commit final offsets.
    c.close()
