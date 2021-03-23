import numpy as np
import string
import json
import time
import uuid
import random
import datetime
import sys
import ssl
import os
from google.cloud import pubsub_v1
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

publisher = pubsub_v1.PublisherClient()

def publish_messages_with_error_handler(project_id, topic_id):
   
    """Publishes multiple messages to a Pub/Sub topic with an error handler."""
  

    # TODO(developer)
 
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    futures = dict()

    def get_callback(f, data):
        def callback(f):
            try:
                print(f.result())
                futures.pop(data)
            except:  # noqa
                print("Please handle {} for {}.".format(f.exception(), data))

        return callback

    coclave_id = str(uuid.uuid4()),
    chars=string.ascii_uppercase + string.digits
    ID = "1CSGCP2GE31EM126762Gabriella"
    def upc():
        message = {
        "name": "PRODUCTO",
        "coclave_id":coclave_id,
        "COCLAVECIC": random.randint(0,121),
        "CODPRODUCTO": ''.join(random.choice(chars) for _ in range(6)),
        "MARGEN_ACTIVO_SOL": random.uniform(0.1, 99.99),    
        "ID": ID,
        "CODMES": str(datetime.datetime.now())
        }
        print ("Message Published" + str(message))

    while True:
        upc()

    for i in range(10):
        data = str(i)
        futures.update({data: None})
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data.encode("utf-8"))
        futures[data] = future
        # Publish failures shall be handled in the callback function.
        future.add_done_callback(get_callback(future, data))

    while futures:
        time.sleep(5)

        print(f"Published messages with error handler to {topic_path}.")