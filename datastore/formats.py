'''
Storage format handlers.
'''

import json
from abc import ABC, abstractmethod

from .errors import StorageFormatConfError, SerializationError, DeserializationError


class FormatBase(ABC):
    '''
    Abstract class for defining storage format.
    All storage formats must be derived from this class.
    Derived class must implement all abstract methods defined in this class.
    '''
    # Format name
    NAME = 'format'
    # Configurations map, conf-name:example-value
    # This map would be used for auto-generating configuration
    CONF = {}

    @abstractmethod
    def serialize(self, record):
        '''
        Serialize record map to storage format.

        Args:
            record (dict): Record map.

        Returns:
            string: Record serialized to the storage format.

        Raises:
            SerializationError: In case serialization fails.
        '''

    @abstractmethod
    def deserialize(self, data):
        '''
        Deserialize data in storage format to record map.

        Args:
            data (string): Data in storage format.

        Returns:
            dict: Record map.

        Raises:
            DeserializationError: In case deserialization fails.
        '''

    @staticmethod
    def _check_confing(user_config, required_config):
        '''
        Helper method to check if any of the required configurations are missing.

        Args:
            user_config (dict): User provided configurations map.
            required_config (dict): Required configurations map.

        Raises:
            StorageFormatConfError: If any configuration is missing.
        '''
        for conf in required_config:
            if conf not in user_config:
                raise StorageFormatConfError(f'Missing configuration "{conf}"')


class JSON(FormatBase):
    '''
    JSON storage format.
    '''
    # Format name
    NAME = 'json'
    # Configurations, no configuration required
    # This map would be used for auto-generating configuration
    CONF = {}

    def __init__(self, **kwargs):
        pass

    def serialize(self, record):
        '''
        Serialize record map to JSON format.

        Args:
            record (dict): Record map.

        Returns:
            string: Record serialized to JSON format.

        Raises:
            SerializationError: In case serialization fails.
        '''
        try:
            return json.dumps(record)
        except (ValueError, TypeError) as err:
            raise SerializationError('Failed to serialize to JSON') from err

    def deserialize(self, data):
        '''
        Deserialize data in JSON format to record map.

        Args:
            data (string): Data in JSON format.

        Returns:
            dict: Record map.

        Raises:
            DeserializationError: In case deserialization fails.
        '''
        try:
            return json.loads(data)
        except (ValueError, TypeError) as err:
            raise DeserializationError('Invalid JSON') from err


class XML(FormatBase):
    '''
    Dummy implementation of XML storage format.
    '''
    # Format name
    NAME = 'XML'
    # Configurations map, configurations required for XML serialize, deserialize.
    CONF = {}

    def __init__(self, **kwargs):
        raise NotImplementedError('This is only a mock implementation of XML format')

    def serialize(self, record):
        '''
        Serialize record map to XML format.

        Args:
            record (dict): Record map.

        Returns:
            string: Record serialized to XML format.

        Raises:
            SerializationError: In case serialization fails.
        '''
        # Convert dict object to XML string

    def deserialize(self, data):
        '''
        Deserialize data in XML format to record map.

        Args:
            data (string): Data in XML format.

        Returns:
            dict: Record map.

        Raises:
            DeserializationError: In case deserialization fails.
        '''
        # Convert XML string to dict object



# Supported formats map
FORMATS = {JSON.NAME: JSON, XML.NAME: XML}


__all__ = ['FORMATS']
