'''
Exceptions for data-store library.
'''

# Exception for all data store errors
class DataStoreError(Exception):
    '''
    Parent exception for all data-store errors.
    '''


# Invalid record error
class InvalidRecord(DataStoreError):
    '''
    Exception raised in case of invalid record.
    '''


# Duplicate record error
class DuplicateRecord(DataStoreError):
    '''
    Exception raised when a record already exists.
    '''


# Storage format errors
class StorageFormatError(DataStoreError):
    '''
    Parent exception for storage format errors.
    '''


class StorageFormatConfError(StorageFormatError):
    '''
    Exception raised in case of missing storage format configuration.
    '''


class SerializationError(StorageFormatError):
    '''
    Exception raised if data serialization fails.
    '''


class DeserializationError(StorageFormatError):
    '''
    Exception raised if data deserialization fails.
    '''


# Storage destination errors
class StorageDestinationError(DataStoreError):
    '''
    Parent exception for storage destination errors.
    '''


class StorageDestinationConfnError(DataStoreError):
    '''
    Exception raised in case of missing storage destination configuration.
    '''


class WriteError(StorageDestinationError):
    '''
    Exception raised if write fails to storage destination.
    '''


class ReadError(StorageDestinationError):
    '''
    Exception raised if read fails on storage destination.
    '''


class NotFoundError(StorageDestinationError):
    '''
    Exception raised if resource not found on storage destination.
    '''
