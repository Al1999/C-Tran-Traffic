#!/usr/bin/env python
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import tokenize
import random
import time
import os
from datetime import date



if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if neede
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    today = date.today()
    d3 = today.strftime("%m-%d-%y")
    name = d3 + '.json'

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))
    count = 0;
    #os.chdir('~/')
    with open(f"../../../../../{name}", "r") as f:
        data = json.load(f)
    for entry in data:
        key = entry["ACT_TIME"]
        value = json.dumps(entry, indent = 4)
        #print("Producing record: {} \t {}".format(key, value))
        producer.produce(topic, key = key, value = value , on_delivery = acked)

        count += 1

        producer.poll(0)
        
    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
'''
    with open("bcsample2.json", "r") as f:
        data = json.loads(f.read())
    for line in data:
        for key in line.keys():
            key = key
            value = line[key]
            print(key, value)
            print("Producing record: {} \t {}".format(key, value))
            producer.produce(topic, key, value , on_delivery = acked)
            producer.poll(0)
'''

