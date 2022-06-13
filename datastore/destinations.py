'''
Storage destination handlers.
'''

import os
from abc import ABC, abstractmethod

from .errors import ReadError, WriteError, NotFoundError
from .errors import StorageDestinationError, StorageDestinationConfnError


class DestinationBase(ABC):
    '''
    Abstract class for defining storage destination.
    All storage destinations must be derived from this class.
    Derived class must implement all abstract methods defined in this class.
    '''
    # Destination name
    NAME = 'destination'
    # Configurations map, conf-name:example-value
    # This map would be used for auto-generating configuration
    CONF = {}

    @abstractmethod
    def init(self):
        '''
        Do necessary initializations and checks.

        Raises:
            StorageDestinationError: In case of error.
        '''

    @abstractmethod
    def store(self, identifier, data):
        '''
        Store a record to storage destination.

        Args:
            identifier (string): Record ID.
            data (string): Record data.

        Raises:
            WriteError: In case of error while writing.
        '''

    @abstractmethod
    def retrieve(self, identifier):
        '''
        Retrieve a record from storage destination.

        Args:
            identifier (string): Record ID.

        Returns:
            string: Record data.

        Raises:
            NotFoundError: In case record is not found.
            ReadError: In case of error while reading.
        '''

    @abstractmethod
    def delete(self, identifier):
        '''
        Delete a record from storage destination.

        Args:
            identifier (string): Record ID.

        Raises:
            NotFoundError: In case record is not found.
            WriteError: In case of error while doing delete.
        '''

    @abstractmethod
    def exists(self, identifier):
        '''
        Check if a record exists in storage destination.

        Args:
            identifier (string): Record ID

        Returns:
            bool: True if record exists else False.

        Raises:
            ReadError: In case of error while checking.
        '''

    @abstractmethod
    def retrieve_all(self):
        '''
        Retrieve all records from storage destination.

        Yields:
            tuple: Record ID and data tuple.

        Raises:
            ReadError: In case of error while reading.
        '''

    @staticmethod
    def _check_confing(user_config, required_config):
        '''
        Helper method to check if any of the required configurations are missing.

        Args:
            user_config (dict): User provided configurations map.
            required_config (dict): Required configurations map.

        Raises:
            StorageDestinationConfnError: If any configuration is missing.
        '''
        for conf in required_config:
            if conf not in user_config:
                raise StorageDestinationConfnError(f'Missing configuration "{conf}"')


class LocalDrive(DestinationBase):
    '''
    Local-drive storage destination.
    '''
    # Destination name
    NAME = 'localdrive'
    # Configurations map, conf-name:example-value
    # This map would be used for auto-generating configuration
    CONF = {'path': './records'}

    def __init__(self, **kwargs):
        '''
        Args:
            path (string): Path for storing records on local drive.
        '''
        # Check configurations
        self._check_confing(kwargs, LocalDrive.CONF)
        self._path = kwargs['path']
        self._encoding = 'utf-8'

    def init(self):
        '''
        Do necessary initializations and checks.

        Raises:
            StorageDestinationError: In case of error.
        '''
        # Create folder if not already present
        if not os.path.exists(self._path):
            try:
                os.makedirs(self._path)
            except OSError as err:
                raise StorageDestinationError(f'Could not create directory {self._path}') from err
        # Check that path is a directory
        elif not os.path.isdir(self._path):
            raise StorageDestinationError(f'{self._path} is not a directory')
        # Check read write permissions on path
        elif not os.access(self._path, os.R_OK) or not os.access(self._path, os.W_OK):
            raise StorageDestinationError(f'Do not have RW permissions on {self._path}')

    def store(self, identifier, data):
        '''
        Store a record to local drive as file.

        Args:
            identifier (string): Record ID.
            data (string): Record data.

        Raises:
            WriteError: In case of error while writing.
        '''
        file_path = os.path.join(self._path, identifier)
        try:
            with open(file_path, mode='w', encoding=self._encoding) as _file:
                _file.write(data)
        except OSError as err:
            raise WriteError(f'Failed to write record {file_path}') from err

    def retrieve(self, identifier):
        '''
        Retrieve a record from local drive.

        Args:
            identifier (string): Record ID.

        Returns:
            string: Record data.

        Raises:
            ReadError: In case of error while reading.
            NotFoundError: In case record is not found.
        '''
        file_path = os.path.join(self._path, identifier)
        try:
            if not os.path.exists(file_path):
                raise NotFoundError(f'Record does not exist {file_path}')
            with open(file_path, encoding=self._encoding) as _file:
                return _file.read()
        except OSError as err:
            raise ReadError(f'Failed to read record {file_path}') from err

    def delete(self, identifier):
        '''
        Delete a record from local drive.

        Args:
            identifier (string): Record ID.

        Raises:
            NotFoundError: In case record is not found.
            WriteError: In case of error while doing delete.
        '''
        file_path = os.path.join(self._path, identifier)
        try:
            if not os.path.exists(file_path):
                raise NotFoundError(f'Record does not exist {file_path}')
            os.remove(file_path)
        except OSError as err:
            raise WriteError(f'Failed to delete record {file_path}') from err

    def exists(self, identifier):
        '''
        Check if a record exists in storage destination.

        Args:
            identifier (string): Record ID

        Returns:
            bool: True if record exists else False.

        Raises:
            ReadError: In case of error while checking.
        '''
        file_path = os.path.join(self._path, identifier)
        try:
            return os.path.exists(file_path)
        except OSError as err:
            raise ReadError(f'Error while checking {file_path} existence') from err

    def retrieve_all(self):
        '''
        Retrieve all records from storage destination.

        Yields:
            tuple: Record ID and data tuple.

        Raises:
            ReadError: In case of error while reading.
        '''
        try:
            for item in os.listdir(self._path):
                file_path = os.path.join(self._path, item)
                with open(file_path) as _file:
                    yield item, _file.read()
        except OSError as err:
            raise ReadError(f'Error while reading from {self._path}') from err


class S3(DestinationBase):
    '''
    Dummy implementation of AWS S3 storage destination.
    '''
    # Destination name
    NAME = 's3'
    # Configurations map, conf-name:example-value
    # This map would be used for auto-generating configuration
    # Add parameters required for connection with S3 bucket
    CONF = {'bucket_name': 'records_bucket'}

    def __init__(self, **kwargs):
        raise NotImplementedError('This is only a mock implementation S3 destination')

    def init(self):
        '''
        Test connection with S3 bucket.

        Raises:
            StorageDestinationError: In case of error.
        '''
        # Try connecting with S3 bucket, raise error in case of failure

    def store(self, identifier, data):
        '''
        Store a record to S3 bucket.

        Args:
            identifier (string): Record ID.
            data (string): Record data.

        Raises:
            WriteError: In case of error while writing.
        '''
        # Upload record object to S3 bucket

    def retrieve(self, identifier):
        '''
        Retrieve a record from S3 bucket.

        Args:
            identifier (string): Record ID.

        Returns:
            string: Record data.

        Raises:
            ReadError: In case of error while reading.
            NotFoundError: In case record is not found.
        '''
        # Get record object from S3 bucket

    def delete(self, identifier):
        '''
        Delete a record from S3 bucket.

        Args:
            identifier (string): Record ID.

        Raises:
            NotFoundError: In case record is not found.
            WriteError: In case of error while doing delete.
        '''
        # Delete record object from S3 bucket

    def exists(self, identifier):
        '''
        Check if a record exists in S3 bucket.

        Args:
            identifier (string): Record ID

        Returns:
            bool: True if record exists else False.

        Raises:
            ReadError: In case of error while checking.
        '''
        # Check if record object exists in S3 bucket

    def retrieve_all(self):
        '''
        Retrieve all records from S3 bucket.

        Yields:
            tuple: Record ID and data tuple.

        Raises:
            ReadError: In case of error while reading.
        '''
        # Retrieve all objects from S3 bucket


# Supported destinations map
DESTINATIONS = {LocalDrive.NAME: LocalDrive, S3.NAME: S3}


__all__ = ['DESTINATIONS']
