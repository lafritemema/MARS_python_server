
from pika import ConnectionParameters, BlockingConnection
from pika.exceptions import AMQPConnectionError
from yaml import load
from .exceptions import ServerException, ServerExceptionType
from typing import List, Callable, Dict, Union
from pika.spec import Queue, BasicProperties, Basic, Channel
import json
import abc
from functools import partial
import logging

class Consumer(metaclass = abc.ABCMeta):
  @abc.abstractmethod
  def __call__(self, body:Dict, headers:Dict):
    pass
  
class CFunction(Consumer):
  def __init__(self, function:Callable[[Dict], Union[Dict, None]], **args):
    self.__name = function.__name__
    self.__function = partial(function, **args)
    self.__logger = logging.getLogger('server.amqp.consumer')
  
  def set_function(self, function:Callable[[Dict], Union[Dict, None]], **args):
    self.__function = partial(function, **args)

  def __call__(self, body:Dict,
               headers:Dict,
               path:str,
               query_args:Dict):

    self.__logger.info(f'|-> run consumer function {self.__name}')
    
    result = self.__function(body=body,
                             headers=headers,
                             path=path,
                             query_args=query_args)
    if result:
      body, headers = result
    
    return body, headers 

  @property
  def name(self):
    return self.__name

class CPipeline(Consumer):
  def __init__(self, cfunction:List[CFunction]):
    self.__pipeline = cfunction
  
  def add_function(self,
                   fonction:CFunction) -> None:
    self.__pipeline.append(function)

  def __call__(self,
               body:Dict,
               headers:Dict,
               path:str,
               query_args:Dict):

    for function in self.__pipeline:
      result = function(body=body,
                        headers=headers,
                        path=path,
                        query_args=query_args)
      if result:
        body, headers = result

class Topic:
  def __init__(self, queue:str,
               consumers:List[Consumer]=None):
    self.__queue = queue
    self.__consumers = consumers if consumers else []
  
  @property
  def consumers(self) -> List[Consumer]:
    return self.__consumers
  
  @property
  def queue(self):
    return self.__queue

  def add_consumer(self, consumer:Consumer) -> None:
    self.__consumers.append(consumer)

class AMQPServer:
  def __init__(self, name:str,
               host:str,
               port:int,
               exchange_name:str,
               exchange_type:str):
    self.__name = name
    self.__connection = AMQPServer.__init_connection(host, port)
    self.__channel = self.__connection.channel()
    self.__channel.exchange_declare(exchange=exchange_name,
                                    exchange_type=exchange_type)
    self.__exchange = exchange_name
    self.__topics:Dict[str, Topic] = {}
    self.__logger = logging.getLogger("server.amqp")

  @property
  def topics(self):
    return self.__topics

  def add_queue(self, label:str, topics:List[str]):
    """function to add a queue to the server and bind topics to this queue

    Args:
        queue_label (str): label for the queue (not equal to the queue name)
        topics (List[str]): list of topics binded to the queue
    """

    #declare the queue in the amqp service provider
    method:Queue.DeclareOk = self.__channel\
                            .queue_declare('', exclusive=False, auto_delete=True)\
                            .method

    # get the queue name
    queue_name = method.queue
    # define a tag with the server name + label
    tag = '.'.join(('ctag', self.__name, label))

    # bind topics to the queue in the amqp service provider
    for topic in topics:
      self.__channel.queue_bind(exchange=self.__exchange,
                                queue=queue_name,
                                routing_key=topic)
      # and add the topic to the server topic collection 
      self.__topics[topic] = Topic(queue_name)
    
    # declare the queue as a consumer in the amqp service provider
    self.__channel.basic_consume(queue=queue_name,
                                 on_message_callback=self.__queue_callback,
                                 auto_ack=True,
                                 consumer_tag=tag)

  def __queue_callback(self,
                       channel:Channel,
                       method:Basic.Deliver,
                       properties:BasicProperties,
                       body:bytes) -> None:
    """callback function called at each message reception

    Args:
        channel (Channel): pika channel object
        method (Basic.Deliver): pika method object
        properties (BasicProperties): pika properties object
        body (bytes): message body

    Raises:
        ServerException: raise if the message is not conform
    """
    try:

      # get the routing key => topic
      routing_key = method.routing_key
      topic = self.__topics.get(routing_key)

      if not (topic):
        raise NotImplementedError(topic)
      consumers = self.__topics[routing_key].consumers


      # content = properties.content_type
      headers = properties.headers
      
      # extract informations from headers
      publisher = headers.pop('publisher')
      path = headers.pop('path')
      # other informations are query arguments
      query_args = headers.copy()
      query_args['Protocol'] = "amqp"

      self.__logger.info(f'message received on topic {routing_key} - publisher : {publisher}')
      
      #get the list of consumers if the topics collection
      topic = self.__topics.get(routing_key)

      # decode and transform in dict
      body_str = body.decode('utf-8')
      body_dict = json.loads(body_str)
      
      # run each consumer function
      for consumer in consumers:
        consumer(body=body_dict.copy(),
                 headers=headers.copy(),
                 path=path,
                 query_args=query_args)

    except json.JSONDecodeError as error:
      # raise if body not in json format
      raise ServerException(['AMQP', 'CONSUME', 'PROCESSING'],
                            ServerExceptionType.INVALID_REQUEST,
                            f"invalid message received from {publisher} : not under json format : {error.msg}",
                            400)
    except KeyError as error:
      # raise if message conform (publisher missing ....)
      raise ServerException(['AMQP', 'CONSUME', 'PROCESSING'],
                            ServerExceptionType.INVALID_REQUEST,
                            "invalid message received: no publisher declared in header",
                            )

    except NotImplementedError as error:
      # raise if message topic not in topic collection
      # raise if message conform (publisher missing ....)

      missing_topic = error.args[0]
      raise ServerException(['AMQP', 'CONSUME', 'PROCESSING'],
                            ServerExceptionType.NOT_IMPLEMENTED,
                            f"no topic {missing_topic} declared in server",
                            501)

  def add_consumer(self, topic:str, consumer:Consumer):
    """function to add consumers to one topic

    Args:
        topic (str): targeted topic
        consumer (Consumer): consumer object to add

    Raises:
        ServerException: raise if topic is not exist
    """
    try:
      # get the topic object from topic collection
      t = self.__topics[topic]
      # add the consumer to topic
      t.add_consumer(consumer)
    except KeyError:
      # raise if topic not exist
      raise ServerException(['AMQP', 'BUILD', 'CONSUMER'],
                            ServerExceptionType.CONFIG_ERROR,
                            f"no topic {topic} declared in the server",
                            501)

  def publish(self, body:Dict, headers:Dict, topic:str=None, **args):
    """function to publish a message to a topic on the amqp service provider 

    Args:
        body (Dict): body of the message
        headers (Dict): headers of the message
        topic (str): the targeted topic
    """
    #define the headers

    if topic:
      report_topic=topic
    elif headers.get("report_topic"):
      report_topic = headers.pop("report_topic")
    else:
      raise ServerException(['SERVER', 'AMQP', 'PUBLISH'],
                            ServerExceptionType.INVALID_REQUEST,
                            "no topic defined to report processing result.",
                            400)   

    headers["publisher"] = self.__name

    # define properties
    prop = BasicProperties(content_type="application/json",
                            headers=headers)

    # encode body dict => string
    body_str = json.dumps(body)

    self.__logger.info(f"publish message on topic {report_topic}")

    # publish the message in the targeted topic in the amqp service provider
    self.__channel.basic_publish(exchange=self.__exchange,
                                 routing_key=report_topic,
                                 properties=prop,
                                 body=body_str)

  @staticmethod
  def __init_connection(host:str, port:int) -> BlockingConnection:
    """function to init a connection with a amqp service provider

    Args:
        host (str): amqp service host
        port (int): amqp service port

    Raises:
        ServerException: raise if amqp service not reachable

    Returns:
        BlockingConnection: pika BlockingConnection
    """
    try:
      con_para = ConnectionParameters(host=host, port=port)
      return BlockingConnection(con_para)
    except AMQPConnectionError as error:
      raise ServerException(['AMQP', 'BUILD', 'CONNECTION'],
                            ServerExceptionType.CONNECTION_ERROR,
                            f"no amqp broker listening on url {host}:{port}",
                            404)
  def run(self):
    """function to run the amqp server"""
    self.__channel.start_consuming()
