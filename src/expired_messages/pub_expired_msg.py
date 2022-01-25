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
publisher for expired messages
"""

import json

import pika

EXCHANGE = "ack-ex"
ROUTING_KEY = "ack-rk"
HOST = "localhost"
QUEUE = "ack-q"
SECONDARY_QUEUE = "secondary-q"
SECONDARY_EXCHANGE = "secondary-ex"
SECONDARY_ROUTING_KEY = "secondary-rk"
TTL = 1000


def pub():
    """
    publish messages
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(HOST))

    channel = connection.channel()

    channel.exchange_declare(EXCHANGE)

    channel.exchange_declare(SECONDARY_EXCHANGE)

    channel.queue_delete(QUEUE)

    args = {
        "x-message-ttl": TTL,
        "x-dead-letter-exchange": SECONDARY_EXCHANGE,
        "x-dead-letter-routing-key": SECONDARY_ROUTING_KEY,
    }

    channel.queue_declare(QUEUE, arguments=args)

    channel.queue_declare(SECONDARY_QUEUE, arguments={})

    channel.queue_bind(QUEUE, EXCHANGE, ROUTING_KEY)

    channel.queue_bind(SECONDARY_QUEUE, SECONDARY_EXCHANGE, SECONDARY_ROUTING_KEY)

    channel.confirm_delivery()

    for i in range(0, 10):
        body = {i: i * i}
        encoded_body = json.dumps(body)
        channel.basic_publish(EXCHANGE, ROUTING_KEY, body=encoded_body)
        print(f"Published {encoded_body} to queue {QUEUE}")

    connection.close()


if __name__ == "__main__":
    pub()
