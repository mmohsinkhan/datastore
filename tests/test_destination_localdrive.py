'''
Unit tests for local-drive storage destination.
'''

import os
import json
import unittest
from uuid import uuid4

from datastore import errors
from datastore.destinations import LocalDrive


class TestDestinationLocalDrive(unittest.TestCase):
    '''
    Unit tests for local-drive storage destination.
    '''

    def setUp(self):
        """
        Create tests data and destination handler instance.
        """
        self._path = './localdrive_records'
        self._destination = LocalDrive(path=self._path)
        self._destination.init()
        self._data = json.dumps({'key1': 1, 'key2': '2', 'key3': 3.0, 'key4': True})

    def test_store(self):
        '''
        Test store function.
        '''
        identifier = uuid4().hex
        self._destination.store(identifier, self._data)
        with open(os.path.join(self._path, identifier)) as _file:
            content = _file.read()
        self.assertEqual(content, self._data, 'Stored data must match with original data')

    def test_retrieve(self):
        '''
        Test retrieve function.
        '''
        identifier = uuid4().hex
        self._destination.store(identifier, self._data)
        retrieved_data = self._destination.retrieve(identifier)
        self.assertEqual(retrieved_data, self._data, 'Retrieved data must match with stored data')

    def test_retrieve_not_found(self):
        '''
        Test retrieve function retrieving record that does not exists.
        '''
        with self.assertRaises(errors.NotFoundError):
            self._destination.retrieve('123')

    def test_delete(self):
        '''
        Test delete function.
        '''
        identifier = uuid4().hex
        self._destination.store(identifier, self._data)
        self._destination.delete(identifier)

    def test_delete_not_found(self):
        '''
        Test delete function trying to delete records that does not exist.
        '''
        with self.assertRaises(errors.NotFoundError):
            self._destination.delete('123')

    def test_exists(self):
        '''
        Test exists function.
        '''
        identifier = uuid4().hex
        self._destination.store(identifier, self._data)
        self.assertTrue(self._destination.exists(identifier), 'Existence status must be True')
        self.assertFalse(self._destination.exists('123'), 'Existence status must be False')

    def test_retrieve_all(self):
        '''
        Test retrieve_all function.
        '''
        stored_records = {}
        identifier = uuid4().hex
        self._destination.store(identifier, self._data)
        stored_records[identifier] = self._data
        identifier = uuid4().hex
        self._destination.store(identifier, self._data)
        stored_records[identifier] = self._data
        retrieved_records = {}
        for identifier, data in self._destination.retrieve_all():
            retrieved_records[identifier] = data
        self.assertEqual(stored_records, retrieved_records, 'Stored and retrieved data must match')

    def tearDown(self):
        '''
        Clear artifacts.
        '''
        if os.path.exists(self._path):
            for item in os.listdir(self._path):
                os.remove(os.path.join(self._path, item))
            os.removedirs(self._path)
