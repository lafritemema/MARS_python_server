import re
from urllib import response
from flask import Flask, request, Response
from werkzeug.exceptions import HTTPException
from typing import Callable, Dict, List
from .exceptions import ServerException, ServerExceptionType
from .validation import Validator
from typing import Union, List, Dict
from functools import partial
import logging

class EFunction():
  def __init__(self,
               function:Callable[[Dict],
               Union[Dict, None]],
               **args):
    self.__name = function.__name__
    self.__function = partial(function, **args)
    self.__logger = logging.getLogger('server.http.endpoint_fct')
    self.__server_type = None
  
  def set_function(self, function:Callable[[Dict], Union[Dict, None]], **args):
    self.__function = partial(function, **args)

  def __call__(self, **args):
    
    self.__logger.info(f'|-> run endpoint function {self.__name}')
    
    # get data from request global var
    body_dict = request.body
    
    query_args = dict(request.headers)
    query_args['Protocol'] = 'http'
    if request.args :
      query_args = query_args.update(request.args)
    
      
    headers = {}
    path = request.path

    #get  data from endpoint function processing 
    result = self.__function(body=body_dict,
                             headers=headers,
                             path=path,
                             query_args=query_args)

    if result:
      body, headers = result

    return body, headers

  @property
  def name(self):
    return self.__name


'''
def build_validator(validator_class):
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for property, subschema in properties.items():
            print(properties, subschema)
            if "default" in subschema:
                instance.setdefault(property, subschema["default"])

        for error in validate_properties(
            validator, properties, instance, schema,
        ):
            yield error

    return validators.extend(
        validator_class, {"properties" : set_defaults},
    )
'''

def server_error_handler(error:ServerException) -> Dict:
  """function to handle server exception error

  Args:
      error (ServerException): error raise by server

  Returns:
      Dict: the error description
  """

  return error.describe(), error.code

def http_error_handler(error:HTTPException):
  if error.code > 500:
    error = ServerException(['SERVER','HTTP', 'REQUEST'],
                          ServerExceptionType.INTERNAL_ERROR,
                          error.description,
                          error.code)
  else:
    error = ServerException(('SERVER', 'HTTP', 'REQUEST'),
                          ServerExceptionType.INVALID_REQUEST,
                          error.description,
                          error.code)

  return server_error_handler(error)

class HttpServer(object):
  """object to describe a http server

  Raises:
      ServerException: raise if an error (configuration, request ...) occured
  """
  app:Flask = None

  def __init__(self, name:str, validator:Validator):
    # initialize flask http server
    self._app = Flask(name)
    # initialize the validatr
    self._validator = validator

    # add error handlers
    self._app.register_error_handler(ServerException, server_error_handler)
    self._app.register_error_handler(HTTPException, http_error_handler)

    # validation for each request
    self._app.before_request(self.__validate)
    self._app.after_request(self.__send_response)

  def __validate(self):
    # raise an error if 
    try:
      path = request.path
      body = request.get_json()

      body = {} if not body else body
      body = self._validator.validate(path, body)

      request.body = body
    except ServerException as error:
      error.add_in_stack(['QUERY'])
      raise error

  def __send_response(self, response:Response)-> Response:
    """function to update response metadata

    Args:
        response (Response): request response

    Returns:
        Response: the updated response
    """
    response.headers.set("Server", self._app.name)
    return response


  def run(self, host, port):
     self._app.run(host=host, port=port)


  def add_endpoint(self, endpoint:str,
                   name:str,
                   handler:EFunction,
                   methods:List[str],
                   validation=True):
    """function to add an endpoint to server

    Args:
        endpoint (str): the endpoint uri
        name (str): endpoint name
        handler (Callable): callback function
        methods (List[str]): http method taked in account
        validation (bool, optional): check validation schema. Defaults to True.

    Raises:
        ServerException: error raise if no schema specified for endpoint in validator
    """
    #check if new endpoint have an associated validation schema, if not raise an error
    if validation and not self._validator.has_key(endpoint):
      raise ServerException(['SERVER','HTTP','CONFIG'],
                            ServerExceptionType.CONFIG_ERROR,
                            f'No validation schema for url {endpoint}, check your schema directory',
                            100)
    
    # add an url rule to flask server
    self._app.add_url_rule(endpoint,
                           name,
                           handler,
                           methods=methods)

