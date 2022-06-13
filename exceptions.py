from exceptions import BaseException
from enum import Enum
from typing import List

class ServerExceptionType(Enum):
  CONNECTION_ERROR = 'SERVER_CONNECTION_ERROR'
  CONFIG_ERROR = 'SERVER_CONFIG_ERROR'
  INVALID_REQUEST = 'SERVER_INVALID_REQUEST_RECEIVED'
  NOT_IMPLEMENTED = 'SERVER_SERVICE_NOT_IMPLEMENTED'
  INTERNAL_ERROR = 'SERVER_INTERNAL_ERROR'

class ServerException(BaseException):
  def __init__(self, origin_stack:List[str],
               _type:ServerExceptionType,
               description:str,
               code:int):
    super().__init__(origin_stack, _type, description)
    self.__code = code

  @property
  def code(self):
    return self.__code
"""
class ServerConfigError(ConfigError):
  def __init__(self, message) -> None:
      super().__init__('SERVER.HTTP', message)


class QueryError(BaseException):
  def __init__(self, message: str, type:str, origin:str):
    self._type = type
    self._origin = origin
    super().__init__(message)
  
  def to_json(self):
    return {
      "type": self._type,
      "origin": self._origin,
      "message": self._message
    }"""