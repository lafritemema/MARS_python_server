from typing import Dict
import fastjsonschema
from typing import Dict
from .exceptions import ServerException, ServerExceptionType

class Validator(object):
  
  def __init__(self):
    self._schema_collection = {}

  def add_schema(self, schema_key:str, schema:Dict):
    self._schema_collection[schema_key] = schema

  def has_key(self, schema_key:str):
    return schema_key in self._schema_collection

  def validate(self, schema_key:str, body:Dict):
    schema = self._schema_collection.get(schema_key)
    validate = fastjsonschema.compile(schema)
    
    try:
      body = validate(body)
      return body
    except fastjsonschema.JsonSchemaException as error:
      raise ServerException(['VALIDATION'],
                            ServerExceptionType.INVALID_REQUEST,
                            f"request body not under json format.\n{error.args[0]}",
                            400)

  