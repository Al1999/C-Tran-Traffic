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

import re
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.request import urlopen




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


    url = "http://www.psudataeng.com:8000/getStopEvents/"
    html = urlopen(url)
    soup = BeautifulSoup(html, 'lxml')

    #extract the date
    head = soup.find('h1')
    x = re.search("\d{4}-\d{1,2}-\d{1,2}", str(head))
    #turn it into a datetime object
    dt = datetime.strptime(x.group(), '%Y-%m-%d')
    finalDate = dt.strftime("%d-%b-%y")

    #trip ids
    tripTitles = soup.findAll('h3')
    tripIds = []
    #get all the tripIds chronologically
    for title in tripTitles:
        title = str(title).replace("h3", "h")
        tripId = re.search("\d+", str(title))
        tripIds.append(tripId.group())

    #prepare the data
    tables = soup.findAll('table')
    table = soup.find('table')
    headings = [th.get_text().strip() for th in table.find("tr").find_all("th")]
    i = 0
    datasets = []
    for table in tables:
        for row in table.findAll("tr")[1:]:
            dataset = dict(zip(headings, (td.get_text() for td in row.find_all("td"))))
            dataset['date'] = finalDate
            dataset['trip_id'] = tripIds[i]
            datasets.append(dataset)
        i+=1


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


    for entry in datasets:
        key = entry["trip_id"]
        value = json.dumps(entry, indent = 2)
        #print("Producing record: {} \t {}".format(key, value))
        producer.produce(topic, key = key, value = value , on_delivery = acked)


        producer.poll(0)
        
    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
