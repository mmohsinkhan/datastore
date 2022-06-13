'''
Data store demonstration app.
'''

import os
import sys
import json
import logging
from uuid import uuid4

from datastore import datastore

# Logger
LOGGER = logging.getLogger('DataStoreDemo')
LOGGER.addHandler(logging.StreamHandler(sys.stdout))
LOGGER.setLevel(logging.DEBUG)


def get_supported_formats_and_destinations():
    '''
    Get supported formats and destinations.
    '''
    LOGGER.info('-- Getting list of supported formats and destinations --\n')
    data = datastore.get_supported_formats_and_destinations()
    LOGGER.info(json.dumps(data, indent=4))
    LOGGER.info('\n')


def get_auto_generated_configuration_template():
    '''
    Get auto generated configuration template for a format-destination pair.
    '''
    LOGGER.info('-- Getting auto generated configuration template for json-localdrive pair --\n')
    configuration = datastore.generate_configuration('json', 'localdrive')
    LOGGER.info(json.dumps(configuration, indent=4))
    LOGGER.info('\n')


def datastore_demo():
    '''
    Data store demo.
    '''
    LOGGER.info('-- Starting data store demo --\n')
    LOGGER.info('Instantiating data store for "json" format and "localdrive" destination')
    # Create data store instance
    datastore_conf = {'format_name':      'json',
                      'format_conf':      {},
                      'destination_name': 'localdrive',
                      'destination_conf': {'path': './demo_records'}}
    _datastore = datastore.DataStore(**datastore_conf)
    # Insert a record
    identifier = uuid4().hex
    record = {'1': 1, '2': 'Two', '3': 3.0}
    LOGGER.info('\nInserting a record: %s', record)
    _datastore.insert(identifier, record)
    # Insert multiple records
    records = {uuid4().hex: {str(i): i} for i in range(3)}
    LOGGER.info('\nInserting multiple records: %s', records)
    _datastore.insert_many(records)
    # Find/retrieve a record
    LOGGER.info('\nRetrieving record %s', identifier)
    retrieved_record = _datastore.find(identifier)
    LOGGER.info('Record found: %s', retrieved_record)
    # Update a record
    update = {'1': 1, '2': 2, '3': 3}
    LOGGER.info('\nUpdating record %s to %s', identifier, update)
    _datastore.update(identifier, update)
    # # Find/retrieve record after update
    LOGGER.info('\nRetrieving record %s after update', identifier)
    retrieved_record = _datastore.find(identifier)
    LOGGER.info('Record found: %s', retrieved_record)
    # Query records
    qfilter = {'1': 1}
    LOGGER.info('\nQuerying records with filter %s', qfilter)
    matches = _datastore.query(qfilter)
    LOGGER.info('Found %d matches', len(matches))
    for item in matches:
        LOGGER.info(item)
    # Query records with different filter
    qfilter = {'1': 1, '2': 2}
    LOGGER.info('\nQuerying records with filter %s', qfilter)
    matches = _datastore.query(qfilter)
    LOGGER.info('Found %d matches', len(matches))
    for item in matches:
        LOGGER.info(item)
    # Query with limit and offset
    qfilter = {'1': 1}
    LOGGER.info('\nQuerying with filter %s and limit 1', qfilter)
    matches = _datastore.query(qfilter, limit=1)
    LOGGER.info('Found %d matches', len(matches))
    for item in matches:
        LOGGER.info(item)
    LOGGER.info('\nQuerying with filter %s, limit 1 and offset 1', qfilter)
    matches = _datastore.query(qfilter, limit=1, offset=1)
    LOGGER.info('Found %d matches', len(matches))
    for item in matches:
        LOGGER.info(item)
    # Empty query filter
    LOGGER.info('\nQuerying with empty query filter "{}"')
    matches = _datastore.query({})
    LOGGER.info('Found %d matches', len(matches))
    for item in matches:
        LOGGER.info(item)
    # Delete a record
    LOGGER.info('\nDeleting record %s', identifier)
    _datastore.delete(identifier)
    # Cleanup
    LOGGER.info('\nDoing cleanup')
    _path = datastore_conf['destination_conf']['path']
    for item in os.listdir(_path):
        os.remove(os.path.join(_path, item))
    os.removedirs(_path)
    LOGGER.info('')


if __name__ == '__main__':
    get_supported_formats_and_destinations()
    get_auto_generated_configuration_template()
    datastore_demo()
