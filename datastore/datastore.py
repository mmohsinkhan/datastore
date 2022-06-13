'''
Library for storing data in multiple storage formats and destinations.
'''

from . import errors
from .formats import FORMATS
from .destinations import DESTINATIONS


class DataStore():
    '''
    Data-store for storing data in multiple formats and destinations.
    '''
    # Supported types for record values
    SUPPORTED_TYPES = (str, int, float, bool)

    def __init__(self, format_name, format_conf, destination_name, destination_conf):
        '''
        Initialize data store providing storage format and destination details.
        Read documentation of format and destination handlers for expected configurations.

        Args:
            format_name (string): Storage format name.
            format_conf (dict): Storage format configurations.
            destination_name (string): Storage destination name.
            destination_conf (dict): Storage destination configurations.

        Raises:
            StorageFormatError: In case of storage format error.
            StorageDestinationError: In case of storage destination error.
        '''
        if format_name not in FORMATS:
            raise errors.StorageFormatError(f'Unknown format "{format_name}"')
        if destination_name not in DESTINATIONS:
            raise errors.StorageDestinationError(f'Unknown destination "{destination_name}"')
        # Create format and destination handlers instance
        self._format = FORMATS[format_name](**format_conf)
        self._destination = DESTINATIONS[destination_name](**destination_conf)
        # Init storage destination
        self._destination.init()

    def insert(self, identifier, data, overwrite=False):
        '''
        Insert a record.

        Args:
            identifier (string): Record ID.
            data (dict): Record data.
            overwrite (bool, optional): Overwrite if record already exists. Defaults to False.

        Raises:
            DuplicateRecord: If record already exists and overwrite is False.
        '''
        if not overwrite and self._destination.exists(identifier):
            raise errors.DuplicateRecord(f'Record {identifier} already exists')
        self._validate_records({identifier: data})
        serialized_data = self._format.serialize(data)
        self._destination.store(identifier, serialized_data)

    def insert_many(self, records, overwrite=False):
        '''
        Insert multiple records.

        Args:
            records (dict): Records (id:data) map.
            overwrite (bool, optional): Overwrite if record already exists. Defaults to False.

        Raises:
            DuplicateRecord: If record already exists and overwrite is False.
        '''
        self._validate_records(records)
        for identifier, data in records.items():
            if not overwrite and self._destination.exists(identifier):
                raise errors.DuplicateRecord(f'Record {identifier} already exists')
            serialized_data = self._format.serialize(data)
            self._destination.store(identifier, serialized_data)

    def find(self, identifier):
        '''
        Find/retrieve a record.

        Args:
            identifier (string): Record ID.

        Returns:
            dict, None: Record data or None if not found.
        '''
        try:
            serialized_data = self._destination.retrieve(identifier)
        except errors.NotFoundError:
            return None
        return self._format.deserialize(serialized_data)

    def update(self, identifier, data, upsert=False):
        '''
        Update a record.

        Args:
            identifier (string): Record ID.
            data (dict): Record data update.
            upsert (bool, optional): Insert record if not present. Defaults to False.

        Raises:
            NotFoundError: If record does not exists and upsert is False.
        '''
        if not upsert and not self._destination.exists(identifier):
            raise errors.NotFoundError(f'Record {identifier} does not exist')
        self._validate_records({identifier: data})
        serialized_data = self._format.serialize(data)
        self._destination.store(identifier, serialized_data)

    def delete(self, identifier, ignore_missing=True):
        '''
        Delete a record.

        Args:
            identifier (string): Record ID.
            ignore_missing (bool, optional): Ignore if record does not exist. Defaults to True.

        Raises:
            NotFoundError: If record does not exists and ignore_missing is False.
        '''
        try:
            self._destination.delete(identifier)
        except errors.NotFoundError:
            if not ignore_missing:
                raise errors.NotFoundError(f'Record {identifier} does not exist')

    def query(self, qfilter, limit=0, offset=0):
        '''
        Retrieve records providing a query filter map.
        Records having attributes matching the query filter shall be return.
        Pass empty query filter '{}' for all records.

        Args:
            qfilter (dict): Query filter map.
            limit (int, optional): Limit. Defaults to 0 (no limit).
            offset (int, optional): Offset. Defaults to 0 (no offset).

        Returns:
            list: List of (record ID, record data) tuples.

        Raises:
            ValueError: In case of invalid limit or offset.
        '''
        if not isinstance(limit, int) or limit < 0:
            raise ValueError('Invalid limit')
        if not isinstance(offset, int) or offset < 0:
            raise ValueError('Invalid offset')
        if limit > 0:
            limit += offset
        matches = []
        # Loop through records
        for identifier, data in self._destination.retrieve_all():
            # Deserialize record
            data = self._format.deserialize(data)
            # Check if record matches query filters
            if qfilter == {} or all(k in data and data[k] == v for k, v in qfilter.items()):
                matches.append((identifier, data))
            if limit and len(matches) == limit:
                break
        return matches[offset:]

    @staticmethod
    def _validate_records(records):
        '''
        Helper method to validate record(s) before insertion/update.

        Args:
            records (dict): Records (id:data) map.

        Raises:
            ValueError: If any of the record is invalid.
        '''
        _types = DataStore.SUPPORTED_TYPES
        for identifier, data in records.items():
            if not isinstance(identifier, str):
                raise errors.InvalidRecord('Record identifier should be of type string')
            if not isinstance(data, dict):
                raise errors.InvalidRecord('Record data should be of type dict/map')
            if not data:
                raise errors.InvalidRecord('Record data should not be empty')
            for name, value in data.items():
                if not isinstance(name, str):
                    raise errors.InvalidRecord('Attribute name should be of type string')
                if not isinstance(value, _types):
                    raise errors.InvalidRecord(f'Attribute value should be one of types {_types}')


def get_supported_formats_and_destinations():
    '''
    Get supported storage formats and destinations.
    '''
    return {'formats': list(FORMATS.keys()), 'destinations': list(DESTINATIONS.keys())}


def generate_configuration(format_name, destination_name):
    '''
    Generate and return configuration template for provide format and destination pair.
    Generated configuration can be directly used for instantiating data-store.

    Args:
        format_name (string): Storage format name.
        destination_name (string): Storage destination name.
    '''
    if format_name not in FORMATS:
        raise errors.StorageFormatError(f'Unknown format "{format_name}"')
    if destination_name not in DESTINATIONS:
        raise errors.StorageDestinationError(f'Unknown destination "{destination_name}"')
    configuration = {'format_name':      format_name,
                     'format_conf':      FORMATS[format_name].CONF,
                     'destination_name': destination_name,
                     'destination_conf': DESTINATIONS[destination_name].CONF}
    return configuration
