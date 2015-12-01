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
import json
from abc import abstractproperty, ABCMeta
from urlparse import urlparse

from pika.exceptions import ChannelClosed

__author__ = 'Fernando Serena'

import StringIO

import uuid
import logging
import pika
from rdflib import Graph, RDF, Literal, BNode, URIRef
from rdflib.namespace import Namespace, FOAF, XSD
from agora.client.agora import Agora
import time
from threading import Thread
from datetime import datetime

log = logging.getLogger('sdh.curator.client')

CURATOR = Namespace('http://www.smartdeveloperhub.org/vocabulary/curator#')
TYPES = Namespace('http://www.smartdeveloperhub.org/vocabulary/types#')
AMQP = Namespace('http://www.smartdeveloperhub.org/vocabulary/amqp#')


class RequestGraph(Graph):
    __metaclass__ = ABCMeta

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

    @abstractproperty
    def type(self):
        pass

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

    def transform(self, elem):
        return elem


class FragmentRequestGraph(RequestGraph):
    __metaclass__ = ABCMeta

    @staticmethod
    def __is_variable(elm):
        return elm.startswith('?')

    def __extend_uri(self, short):
        """
        Extend a prefixed uri with the help of a specific dictionary of prefixes
        :param short: Prefixed uri to be extended
        :return:
        """
        if short == 'a':
            return RDF.type
        for prefix in sorted(self.__prefixes, key=lambda x: len(x), reverse=True):
            if short.startswith(prefix):
                return URIRef(short.replace(prefix + ':', self.__prefixes[prefix]))
        return short

    def is_uri(self, uri):
        if uri.startswith('<') and uri.endswith('>'):
            uri = uri.lstrip('<').rstrip('>')
            parse = urlparse(uri, allow_fragments=True)
            return bool(len(parse.scheme))
        elif ':' in uri:
            prefix_parts = uri.split(':')
            return len(prefix_parts) == 2 and prefix_parts[0] in self.__prefixes

        return uri == 'a'

    def __init__(self, *args, **kwargs):
        super(FragmentRequestGraph, self).__init__()
        if not args:
            raise AttributeError('A graph pattern must be provided')

        self.__prefixes = kwargs.get('prefixes', None)
        if self.__prefixes is None:
            raise AttributeError('A prefixes list must be provided')

        elements = {}

        for tp in args:
            s, p, o = tuple(tp.strip().split(' '))
            if s not in elements:
                if self.__is_variable(s):
                    elements[s] = BNode(s)
                    self.set((elements[s], RDF.type, CURATOR.Variable))
                    self.set((elements[s], CURATOR.label, Literal(s, datatype=XSD.string)))
            if p not in elements:
                if self.is_uri(p):
                    elements[p] = self.__extend_uri(p)
            if o not in elements:
                if self.__is_variable(o):
                    elements[o] = BNode(o)
                    self.set((elements[o], RDF.type, CURATOR.Variable))
                    self.set((elements[o], CURATOR.label, Literal(o, datatype=XSD.string)))
                elif self.is_uri(o):
                    elements[o] = self.__extend_uri(o)
                else:
                    elements[o] = Literal(o)

            self.add((elements[s], elements[p], elements[o]))


class StreamRequestGraph(FragmentRequestGraph):
    def __init__(self, *args, **kwargs):
        super(StreamRequestGraph, self).__init__(*args, **kwargs)
        self.add((self.request_node, RDF.type, CURATOR.StreamRequest))

    @property
    def type(self):
        return 'stream'

    def transform(self, quad):
        def __transform(x):
            if type(x) == str or type(x) == unicode:
                if self.is_uri(x):
                    return URIRef(x.lstrip('<').rstrip('>'))
                elif '^^' in x:
                    (value, ty) = tuple(x.split('^^'))
                    return Literal(value.replace('"', ''), datatype=URIRef(ty.lstrip('<').rstrip('>')))
                else:
                    return Literal(x.replace('"', ''), datatype=XSD.string)
            return x

        triple = quad[1:]
        return tuple([quad[0]] + map(__transform, triple))


class QueryRequestGraph(FragmentRequestGraph):
    def __init__(self, *args, **kwargs):
        super(QueryRequestGraph, self).__init__(*args, **kwargs)
        self.add((self.request_node, RDF.type, CURATOR.QueryRequest))

    @property
    def type(self):
        return 'query'


class CuratorClient(object):
    def __init__(self, broker_host='localhost', broker_port=5672, wait=False, monitoring=None, agora_host='localhost',
                 agora_port=5002, stop_event=None):
        self.agora = Agora(host=agora_host, port=agora_port)
        self.__connection = pika.BlockingConnection(pika.ConnectionParameters(host=broker_host, port=broker_port))
        self.__channel = self.__connection.channel()
        self.__listening = False
        self.__accept_queue = self.__response_queue = None
        self.__monitor = Thread(target=self.__monitor_consume, args=[monitoring]) if monitoring else None
        self.__last_consume = datetime.now()
        self.__keep_monitoring = True
        self.__accepted = False
        self.__message = None
        self.__wait = wait
        self.__stop_event = stop_event

    def __monitor_consume(self, t):
        log.debug('Curator client monitor started...')
        while self.__keep_monitoring:
            if (datetime.now() - self.__last_consume).seconds > t:
                self.stop()
                break
            else:
                time.sleep(1)

    def request(self, message):
        self.__response_queue = self.__channel.queue_declare(auto_delete=True).method.queue
        message.routing_key = self.__response_queue
        self.__message = message

        self.__accept_queue = self.__channel.queue_declare(auto_delete=True).method.queue
        self.__channel.queue_bind(exchange='sdh', queue=self.__accept_queue,
                                  routing_key='curator.response.{}'.format(str(message.agent_id)))

        self.__channel.basic_publish(exchange='sdh',
                                     routing_key='curator.request.{}'.format(self.__message.type),
                                     body=message.serialize(format='turtle'))
        self.__listening = True
        return self.agora.prefixes, self.__consume()

    def __consume(self):
        def __response_callback(properties, body):
            if properties.headers.get('state', None) == 'end':
                log.info('End of stream received!')
                self.stop()
            else:
                try:
                    items = json.loads(body)
                except ValueError:
                    items = eval(body)

                if not isinstance(items, list):
                    items = [items]
                for item in items:
                    yield properties.headers, item

        for message in self.__channel.consume(self.__accept_queue, no_ack=True, inactivity_timeout=1):
            if message is not None:
                method, properties, body = message
                g = Graph()
                g.parse(StringIO.StringIO(body), format='turtle')
                if len(list(g.subjects(RDF.type, CURATOR.Accepted))) == 1:
                    log.info('Request accepted!')
                    self.__accepted = True
                else:
                    log.error('Bad request!')
                self.__channel.queue_delete(self.__accept_queue)
                self.__channel.cancel()
                break
            elif self.__stop_event is not None:
                if self.__stop_event.isSet():
                    self.stop()
        if not self.__accepted:
            log.debug('Request not accepted. Aborting...')
            raise StopIteration()
        if self.__monitor is not None:
            self.__monitor.start()
        for message in self.__channel.consume(self.__response_queue, no_ack=True, inactivity_timeout=1):
            if message is not None:
                method, properties, body = message
                for headers, item in __response_callback(properties, body):
                    yield headers, self.__message.transform(item)
            elif not self.__wait:
                yield None
            elif self.__stop_event is not None:
                if self.__stop_event.isSet():
                    self.stop()
                    raise StopIteration()
            else:
                log.debug('Inactivity timeout...')
            self.__last_consume = datetime.now()
        if self.__monitor is not None:
            self.__keep_monitoring = False
            log.debug('Waiting for client monitor to stop...')
            self.__monitor.join()

    def stop(self):
        try:
            self.__channel.queue_delete(self.__accept_queue)
            self.__channel.queue_delete(self.__response_queue)
            self.__channel.cancel()
            self.__channel.close()
            self.__listening = False
        except ChannelClosed:
            pass
        log.debug('Stopped curator client!')

    @property
    def listening(self):
        return self.__listening


def get_fragment_generator(*args, **kwargs):
    client = CuratorClient(**kwargs)
    request = StreamRequestGraph(prefixes=client.agora.prefixes, *args)
    request.broker_host = kwargs['broker_host']
    return client.request(request)


def get_query_generator(*args, **kwargs):
    client = CuratorClient(**kwargs)
    request = QueryRequestGraph(prefixes=client.agora.prefixes, *args)
    request.broker_host = kwargs['broker_host']
    return client.request(request)
