'''
Unit tests for data store class.
'''

import os
import unittest
from uuid import uuid4

from datastore import errors
from datastore.datastore import DataStore


class TestDataStoreJSONtoLocalDrive(unittest.TestCase):
    '''
    Unit tests for data store json-localdrive pair.
    '''

    def setUp(self):
        """
        Create instance of data store for json-localdrive pair.
        """
        self._path = './datastore_records'
        data_store_conf = {'format_name': 'json',
                           'format_conf': {},
                           'destination_name': 'localdrive',
                           'destination_conf': {'path': self._path}}
        self._datastore = DataStore(**data_store_conf)
        self._data = {'key1': 1, 'key2': '2', 'key3': 3.0, 'key4': True}

    def test_insert(self):
        '''
        Test insert function inserting new record.
        '''
        identifier = uuid4().hex
        self._datastore.insert(identifier, self._data)
        file_path = os.path.join(self._path, identifier)
        self.assertTrue(os.path.exists(file_path), 'Record does not exists')       

    def test_insert_on_existing_record_with_overwrite_false(self):
        '''
        Test insert function inserting record that already existing, with overwrite set to False.
        '''
        identifier = uuid4().hex
        self._datastore.insert(identifier, self._data)
        with self.assertRaises(errors.DuplicateRecord):
            self._datastore.insert(identifier, self._data, overwrite=False)

    def test_insert_on_existing_record_with_overwrite_true(self):
        '''
        Test insert function inserting record that already exist, with overwrite set to True.
        '''
        identifier = uuid4().hex
        self._datastore.insert(identifier, self._data)
        self._datastore.insert(identifier, self._data, overwrite=True)

    def test_insert_many(self):
        '''
        Test insert_many function inserting new records.
        '''
        records = {uuid4().hex: {str(i): i} for i in range(3)}
        self._datastore.insert_many(records)
        files = os.listdir(self._path)
        self.assertTrue(all(identifier in files for identifier in records), "Missing records")

    def test_insert_many_on_existing_records_with_overwrite_false(self):
        '''
        Test insert_many function inserting records that already exist, with overwrite set
        to False.
        '''
        records = {uuid4().hex: {str(i): i} for i in range(3)}
        self._datastore.insert_many(records)
        with self.assertRaises(errors.DuplicateRecord):
            self._datastore.insert_many(records, overwrite=False)

    def test_insert_many_on_existing_records_with_overwrite_true(self):
        '''
        Test insert_many function inserting records that already exist, with overwrite set
        to True.
        '''
        records = {uuid4().hex: {str(i): i} for i in range(3)}
        self._datastore.insert_many(records)
        self._datastore.insert_many(records, overwrite=True)

    def test_find(self):
        '''
        Test find function on and existing record.
        '''
        identifier = uuid4().hex
        self._datastore.insert(identifier, self._data)
        record = self._datastore.find(identifier)
        self.assertEqual(record, self._data, 'Find must return data that was inserted')

    def test_find_on_non_existing_record(self):
        '''
        Test find function to find a record that does not exist.
        '''
        record = self._datastore.find('123')
        self.assertEqual(record, None, 'Find must return None if record does not exist')

    def test_update(self):
        '''
        Test update function updating a record.
        '''
        identifier = uuid4().hex
        self._datastore.insert(identifier, self._data)
        update = {'one': 1, 'two': 2}
        self._datastore.update(identifier, update)
        updated_record = self._datastore.find(identifier)
        self.assertEqual(updated_record, update, 'Record must get updated after update')

    def test_update_on_non_existing_record_with_upsert_false(self):
        '''
        Test update function on non existing record with upsert set to False.
        '''
        with self.assertRaises(errors.NotFoundError):
            self._datastore.update('123', {"1": 1}, upsert=False)

    def test_update_on_non_existing_record_with_upsert_true(self):
        '''
        Test update function on non existing record with upsert set to True.
        '''
        identifier = uuid4().hex
        update = {'one': 1, 'two': 2}
        self._datastore.update(identifier, update, upsert=True)
        record = self._datastore.find(identifier)
        self.assertEqual(record, update, 'Update must insert record if upsert is True')

    def test_delete(self):
        '''
        Test delete function on an existing record.
        '''
        identifier = uuid4().hex
        self._datastore.insert(identifier, self._data)
        self._datastore.delete(identifier)
        record = self._datastore.find(identifier)
        self.assertEqual(record, None, 'Find should return None after record deletion')

    def test_delete_on_non_existing_record_with_ignore_missing_false(self):
        '''
        Test delete function trying to delete a non existing record with ignore_missing set
        to False.
        '''
        with self.assertRaises(errors.NotFoundError):
            self._datastore.delete('123', ignore_missing=False)

    def test_delete_on_non_existing_record_with_ignore_missing_true(self):
        '''
        Test delete function trying to delete a non existing record with ignore_missing set
        to True.
        '''
        self._datastore.delete('123', ignore_missing=True)

    def test_query(self):
        '''
        Test query function querying records with different query filters.
        '''
        records = {'ID1': {'value': 1},
                   'ID2': {'value': 1, 'name': 'test'},
                   'ID3': {'value': 1, 'name': 'test1'},
                   'ID4': {'value': 2}}
        self._datastore.insert_many(records)
        qfilter = {'value': 1}
        matches = self._datastore.query(qfilter)
        # IDs that should be matched
        ids = {'ID1', 'ID2', 'ID3'}
        self.assertEqual({item[0] for item in matches}, ids, 'Wrong query result')
        qfilter = {'value': 1, 'name': 'test1'}
        matches = self._datastore.query(qfilter)
        # IDs that should be matched
        ids = {'ID3'}
        self.assertEqual({item[0] for item in matches}, ids, 'Wrong query result')
        # Test empty query filter.
        matches = self._datastore.query({})
        self.assertEqual(len(records), len(matches), 'Empty query filter should return all record')

    def test_query_with_limit(self):
        '''
        Test query function with query filter and limit.
        '''
        records = {'ID1': {'value': 1},
                   'ID2': {'value': 1, 'name': 'test'},
                   'ID3': {'value': 1, 'name': 'test1'},
                   'ID4': {'value': 2}}
        self._datastore.insert_many(records)
        qfilter = {'value': 1}
        matches = self._datastore.query(qfilter, limit=0)
        self.assertEqual(len(matches), 3, 'All matches must be returned for limit 0')
        matches = self._datastore.query(qfilter, limit=100)
        self.assertEqual(len(matches), 3, 'All matches must be returned if limit > matches')
        matches = self._datastore.query(qfilter, limit=1)
        self.assertEqual(len(matches), 1, 'Max 1 match must be returned for limit 1')

    def test_query_with_limit_and_offset(self):
        '''
        Test query function with query filter, limit and offset.
        '''
        records = {'ID1': {'value': 1},
                   'ID2': {'value': 1, 'name': 'test'},
                   'ID3': {'value': 1, 'name': 'test1'},
                   'ID4': {'value': 2}}
        self._datastore.insert_many(records)
        qfilter = {'value': 1}
        matches = self._datastore.query(qfilter, limit=0, offset=1)
        self.assertEqual(len(matches), 2, 'Matches equal to the offset must be excluded')
        matches = self._datastore.query(qfilter, limit=0, offset=0)
        self.assertEqual(len(matches), 3, 'All matches should be returned with 0 offset and limit')
        matches = self._datastore.query(qfilter, limit=0, offset=10)
        self.assertEqual(len(matches), 0, 'No record should be returned if offset > matches')

    def test_limit_and_offset_invalid_values(self):
        '''
        Test query function for invalid limit and offset values.
        '''
        with self.assertRaises(ValueError):
            self._datastore.query({}, limit=-1)
        with self.assertRaises(ValueError):
            self._datastore.query({}, limit=None)
        with self.assertRaises(ValueError):
            self._datastore.query({}, limit='limit')
        with self.assertRaises(ValueError):
            self._datastore.query({}, offset=-1)
        with self.assertRaises(ValueError):
            self._datastore.query({}, offset=None)
        with self.assertRaises(ValueError):
            self._datastore.query({}, offset='offset')

    def tearDown(self):
        '''
        Clear artifacts.
        '''
        if os.path.exists(self._path):
            for item in os.listdir(self._path):
                os.remove(os.path.join(self._path, item))
            os.removedirs(self._path)


if __name__ == '__main__':
    unittest.main()
