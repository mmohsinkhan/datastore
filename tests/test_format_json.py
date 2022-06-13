'''
Unit tests for JSON storage format.
'''

import json
import unittest

from datastore import errors
from datastore.formats import JSON


class TestFormatJSON(unittest.TestCase):
    '''
    Unit tests for JSON storage format.
    '''

    def setUp(self):
        """
        Create tests data and format handler instance.
        """
        self._format = JSON()
        self._data = {'key1': 1, 'key2': '2', 'key3': 3.0, 'key4': True}

    def test_serialization(self):
        '''
        Test serialization function with valid data.
        '''
        serialized_data = self._format.serialize(self._data)
        self.assertEqual(json.loads(serialized_data), self._data, 'Incorrect JSON serialization')

    def test_serialization_invalid_data(self):
        '''
        Test serialization function with invalid data.
        '''
        with self.assertRaises(errors.SerializationError):
            self._format.serialize({1})

    def test_deserialization(self):
        '''
        Test deserialization function with valid data.
        '''
        deserialized_data = self._format.deserialize(json.dumps(self._data))
        self.assertEqual(deserialized_data, self._data, 'Incorrect JSON deserialization')

    def test_deserialization_invalid_data(self):
        '''
        Test deserialization function with invalid data.
        '''
        with self.assertRaises(errors.DeserializationError):
            self._format.deserialize('test')
