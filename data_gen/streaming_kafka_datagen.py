from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from datetime import datetime
from random import choice
import time
import string
import random
import json

################################
## MESSAGE CONFIG SETTINGS 
##
## number of messages to push
num_messages = 9000
##
## delay between messages in sec
delay=.1 #100 MS or 10 per sec
##
##################################

if __name__ == '__main__':
     # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('topic_name', type=str)
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    ## which topic to push to
    topic = args.topic_name

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


    #random date generator function
    def random_date(seed):
        random.seed(seed)
        d = random.randint(1, int(time.time()))
        return datetime.fromtimestamp(d).strftime('%Y-%m-%d')


    
    count = 0
    for _ in range(num_messages):

        json_raw = {
        "transaction_id": f'{count:09}',
        "transaction_date": datetime.now().strftime("%m/%d/%Y %I:%M:%S.%f %p"),
        "vendor_id": random.randint(1, 6),
        "product_id": random.randint(10000000, 99999999),
        "product_price": random.randint(9, 2000),
        "quantity": random.randint(1, 20),
        "product_name": ''.join(random.choices(string.ascii_lowercase, k=random.randint(4, 10))).capitalize(),
        "product_desc": ''.join(random.choices(string.ascii_lowercase, k=random.randint(6, 20))).capitalize()
        }
        json_data = json.dumps(json_raw)
        key=str(count)
     
        producer.produce(topic, json_data, key, callback=delivery_callback)
        count += 1
        time.sleep(delay)


    # Block until the messages are sent.
    #producer.poll(10000)
    producer.flush()