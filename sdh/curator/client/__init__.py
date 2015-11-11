"""
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  This file is part of the Smart Developer Hub Project:
    http://www.smartdeveloperhub.org

  Center for Open Middleware
        http://www.centeropenmiddleware.com/
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Copyright (C) 2015 Center for Open Middleware.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
"""

__author__ = 'Fernando Serena'

import StringIO

import uuid
from datetime import datetime

import pika
from rdflib import Graph, RDF, Literal, BNode
from rdflib.namespace import Namespace, FOAF
import re

CURATOR = Namespace('http://www.smartdeveloperhub.org/vocabulary/curator#')
TYPES = Namespace('http://www.smartdeveloperhub.org/vocabulary/types#')
AMQP = Namespace('http://www.smartdeveloperhub.org/vocabulary/amqp#')


class RequestGraph(Graph):
    def __init__(self):
        super(RequestGraph, self).__init__()
        self._request_node = BNode()
        self._agent_node = BNode()
        self._broker_node = BNode()
        self._channel_node = BNode()
        self._message_id = self._agent_id = self._submitted_on = self._exchange_name = None
        self._routing_key = self._broker_host = self._broker_port = self._broker_vh = None

        # Node binding
        self.add((self.request_node, CURATOR.replyTo, self.channel_node))
        self.add((self.request_node, CURATOR.submittedBy, self.agent_node))
        self.add((self.channel_node, RDF.type, CURATOR.DeliveryChannel))
        self.add((self.broker_node, RDF.type, AMQP.Broker))
        self.add((self.channel_node, AMQP.broker, self.broker_node))
        self.add((self.agent_node, RDF.type, FOAF.Agent))

        # Default graph
        self.message_id = uuid.uuid4()
        self.submitted_on = datetime.now()
        self.agent_id = uuid.uuid4()
        self.exchange_name = ""
        self.routing_key = ""
        self.broker_host = "localhost"
        self.broker_port = 5672
        self.broker_vh = "/"

        self.bind('curator', CURATOR)
        self.bind('amqp', AMQP)
        self.bind('foaf', FOAF)
        self.bind('types', TYPES)

    @property
    def request_node(self):
        return self._request_node

    @property
    def broker_node(self):
        return self._broker_node

    @property
    def channel_node(self):
        return self._channel_node

    @property
    def agent_node(self):
        return self._agent_node

    @property
    def message_id(self):
        return self._message_id

    @message_id.setter
    def message_id(self, value):
        self._message_id = Literal(str(value), datatype=TYPES.UUID)
        self.set((self._request_node, CURATOR.messageId, self._message_id))

    @property
    def agent_id(self):
        return self._agent_id

    @agent_id.setter
    def agent_id(self, value):
        self._agent_id = Literal(str(value), datatype=TYPES.UUID)
        self.set((self._agent_node, CURATOR.agentId, self._agent_id))

    @property
    def submitted_on(self):
        return self._submitted_on

    @submitted_on.setter
    def submitted_on(self, value):
        self._submitted_on = Literal(value)
        self.set((self._request_node, CURATOR.submittedOn, self._submitted_on))

    @property
    def exchange_name(self):
        return self._exchange_name

    @exchange_name.setter
    def exchange_name(self, value):
        self._exchange_name = Literal(value, datatype=TYPES.Name)
        self.set((self.channel_node, AMQP.exchangeName, self._exchange_name))

    @property
    def routing_key(self):
        return self._routing_key

    @routing_key.setter
    def routing_key(self, value):
        self._routing_key = Literal(value, datatype=TYPES.Name)
        self.set((self.channel_node, AMQP.routingKey, self._routing_key))

    @property
    def broker_host(self):
        return self._broker_host

    @broker_host.setter
    def broker_host(self, value):
        self._broker_host = Literal(value, datatype=TYPES.Hostname)
        self.set((self.broker_node, AMQP.host, self._broker_host))

    @property
    def broker_port(self):
        return self._broker_port

    @broker_port.setter
    def broker_port(self, value):
        self._broker_port = Literal(value, datatype=TYPES.Port)
        self.set((self.broker_node, AMQP.port, self._broker_port))

    @property
    def broker_vh(self):
        return self._broker_vh

    @broker_vh.setter
    def broker_vh(self, value):
        self._broker_vh = Literal(value, datatype=TYPES.Path)
        self.set((self.broker_node, AMQP.virtualHost, self._broker_vh))


class StreamRequestGraph(RequestGraph):
    def __init__(self, *args):
        super(StreamRequestGraph, self).__init__()
        self.add((self.request_node, RDF.type, CURATOR.StreamRequest))
        if not args:
            raise AttributeError('A graph pattern must be provided')

        for tp in args:
            s, p, o = tuple(tp.split(' '))
            # if s.startswith('?'):




class CuratorClient(object):
    def __init__(self, host='localhost', port=5672):
        self.__host = host
        self.__port = port
        self.__connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
        self.__channel = self.__connection.channel()

    def request(self, message, callback):
        def __accept_callback(ch, method, properties, body):
            g = Graph()
            g.parse(StringIO.StringIO(body), format='turtle')
            print g.serialize(format='turtle')
            if len(list(g.subjects(RDF.type, CURATOR.Accepted))) == 1:
                print 'Request accepted!'
            else:
                print 'Bad request!'

        def __response_callback(ch, method, properties, body):
            callback(body)

        response_queue = self.__channel.queue_declare(auto_delete=True).method.queue
        message.routing_key = response_queue
        self.__channel.basic_consume(__response_callback, queue=response_queue, no_ack=True)

        accept_queue = self.__channel.queue_declare(exclusive=True).method.queue
        self.__channel.queue_bind(exchange='sdh', queue=accept_queue,
                                  routing_key='curator.response.{}'.format(str(message.agent_id)))
        self.__channel.basic_consume(__accept_callback, queue=accept_queue, no_ack=True)

        self.__channel.basic_publish(exchange='sdh',
                                     routing_key='curator.request.stream',
                                     body=message.serialize(format='turtle'))

    def start(self):
        self.__channel.start_consuming()

    def stop(self):
        self.__channel.stop_consuming()
