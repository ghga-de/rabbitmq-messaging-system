# Copyright 2021 Universität Tübingen, DKFZ and EMBL
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
subscriber for negatively acknowledged messages
"""

import json
import os
import sys

import pika

EXCHANGE = "ack-ex"
ROUTING_KEY = "ack-rk"
HOST = "localhost"
QUEUE = "ack-q"


def sub():
    """
    subcriber for messages
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE)

    channel.basic_consume(queue=QUEUE, on_message_callback=callback)

    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


def callback(chan, method, _properties, body):
    """
    callback for messages

    Args:
        chan ([type]): [description]
        method ([type]): [description]
        _properties ([type]): [description]
        body ([type]): [description]
    """
    msg = json.loads(body.decode())
    for key in msg:
        if msg[key] % 2 != 0:
            print(
                f"Received {msg} and"
                + f"acknowledging the message with delivery tag {method.delivery_tag}"
            )
            chan.basic_ack(method.delivery_tag)
        else:
            print(
                "Negative acknowledgment"
                + f"sent for the message with delivery tag {method.delivery_tag}"
            )
            chan.basic_nack(method.delivery_tag)


if __name__ == "__main__":
    try:
        sub()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)  # pylint: disable=W0212
