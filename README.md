# Data Store Library

Library for storing arbitrary data (records) in multiple formats and destinations.

For example
- Storing data in JSON format to a folder on local drive.
- Storing data in XML format to S3 bucket.

Features
- Data storage in any of the supported formats and destinations.
- Retrieval of stored data.
- Query on stored data using query filter, with limit and offset.
- Update and delete stored data.

# Supported Formats and Destinations

Storage Formats
- json
- xml (only mock implementation)

Storage Destinations
- localdrive
- s3 (only mock implementation)

# Dependencies

Python
- Tested with Python 3.8.

Packages
- As per the requirement of storage format and destination handlers. (No extra package is required for current implementation).


# Implementation Details

Abstract classes are defined for format and destination handlers.
```
# Abstract class for format handlers
datastore.formats.FormatBase

# Abstract class for destination handlers
datastore.destinations.DestinationBase
```
These abstract classes defines a standard interface for implementing handlers.
```
# Interface of format handler
serialize: Convert record object to storage format e.g JSON
deserialize: Convert data in storage format e.g. JSON to record object

# Interface of destination handler
init: Initialize/check storage destination
store: Store a record to storage destination
retrieve: Retrieve a record from storage destination
delete: Delete a record from storage destination
exists: Check if a record exists in storage destination
retrieve_all: Retrieve all records from storage destination
```
Format and destination handlers are derived from their abstract class and implements the required interface.
```
# JSON format handler
datastore.formats.JSON

# Local drive destination handler
datastore.destinations.LocalDrive
```
Data store is also implemented as a class that expects format & destination name and configuration on instantiation. Because of standard interface of format and destination handlers, new handlers can be added to the library without changing data store class code.
```
# Data store class
datastore.datastore.DataStore

# Interface of data store
insert: Insert a record
insert_many: Insert multiple records 
find: Retrieve a record
update: Update a record
delete: Delete a record
query: Retrieve records using query filter
```
List of supported format and destination handlers can be fetched using provided helper function.
```py
from datastore import datastore

data = datastore.get_supported_formats_and_destinations()
print(data)

# Output
{
    "formats": [
        "json",
        "XML"
    ],
    "destinations": [
        "localdrive",
        "s3"
    ]
}
```
Configuration template for a format and destination pair can be auto generated using provided helper function.
```py
from datastore import datastore

data = datastore.generate_configuration('json', 'localdrive')
print(data)

# Output
{
    "format_name": "json",
    "format_conf": {},
    "destination_name": "localdrive",
    "destination_conf": {
        "path": "./records"
    }
}
```
Custom exceptions are defined in *errors* module.
```
# Custom exceptions
datastore.datastore.errors

# Base exceptions
DataStoreError: Exceptions for all errors from the library
StorageFormatError: Exception for all storage format related errors, derived from DataStoreError
StorageDestinationError: Exception for all storage destination related errors, derived from DataStoreError
```

# Source Code
```
datastore/                              # Data store library
    __init__.py
    datastore.py                        # Interface of data-store library
    errors.py                           # Custom exceptions
    destinations.py                     # Destination handlers implementation
    formats.py                          # Format handlers implementation
tests/
    test_datastore_json_localdrive.py   # Unit tests for json-localdrive pair
    test_destination_localdrive.py      # Unit tests for localdrive destination handler
    test_format_json.py                 # Unit tests for json format handler
demo.py                                 # Demonstration app
```

# Usage

- Clone and checkout *datastore* repo.
- Change directory to *datastore* directory.
```sh
cd datastore
```
- Run unit tests.
```sh
python -m unittest discover -v
```
- Run demonstration app *demo.py*.
```sh
python demo.py
```
- See *demo.py* app code for usage details.

Demo app log
```
-- Getting list of supported formats and destinations --

{
    "formats": [
        "json",
        "XML"
    ],
    "destinations": [
        "localdrive",
        "s3"
    ]
}


-- Getting auto generated configuration template for json-localdrive pair --

{
    "format_name": "json",
    "format_conf": {},
    "destination_name": "localdrive",
    "destination_conf": {
        "path": "./records"
    }
}


-- Starting data store demo --

Instantiating data store for "json" format and "localdrive" destination

Inserting a record: {'1': 1, '2': 'Two', '3': 3.0}

Inserting multiple records: {'9325c4fd5ddf414784ef5e82dabe6962': {'0': 0}, '3813e99018be4a4aac73a69976df3390': {'1': 1}, 'd1fd20bfab174a9abcb01d6ac41eb3d0': {'2': 2}}

Retrieving record 860bea8e04044122a2bc961283f32035
Record found: {'1': 1, '2': 'Two', '3': 3.0}

Updating record 860bea8e04044122a2bc961283f32035 to {'1': 1, '2': 2, '3': 3}

Retrieving record 860bea8e04044122a2bc961283f32035 after update
Record found: {'1': 1, '2': 2, '3': 3}

Querying records with filter {'1': 1}
Found 2 matches
('3813e99018be4a4aac73a69976df3390', {'1': 1})
('860bea8e04044122a2bc961283f32035', {'1': 1, '2': 2, '3': 3})

Querying records with filter {'1': 1, '2': 2}
Found 1 matches
('860bea8e04044122a2bc961283f32035', {'1': 1, '2': 2, '3': 3})

Querying with filter {'1': 1} and limit 1
Found 1 matches
('3813e99018be4a4aac73a69976df3390', {'1': 1})

Querying with filter {'1': 1}, limit 1 and offset 1
Found 1 matches
('860bea8e04044122a2bc961283f32035', {'1': 1, '2': 2, '3': 3})

Querying with empty query filter "{}"
Found 4 matches
('9325c4fd5ddf414784ef5e82dabe6962', {'0': 0})
('3813e99018be4a4aac73a69976df3390', {'1': 1})
('860bea8e04044122a2bc961283f32035', {'1': 1, '2': 2, '3': 3})
('d1fd20bfab174a9abcb01d6ac41eb3d0', {'2': 2})

Deleting record 860bea8e04044122a2bc961283f32035

Doing cleanup
```

Unit tests execution log
```
test_delete (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test delete function on an existing record. ... ok
test_delete_on_non_existing_record_with_ignore_missing_false (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test delete function trying to delete a non existing record with ignore_missing set ... ok
test_delete_on_non_existing_record_with_ignore_missing_true (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test delete function trying to delete a non existing record with ignore_missing set ... ok
test_find (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test find function on and existing record. ... ok
test_find_on_non_existing_record (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test find function to find a record that does not exist. ... ok
test_insert (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test insert function inserting new record. ... ok
test_insert_many (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test insert_many function inserting new records. ... ok
test_insert_many_on_existing_records_with_overwrite_false (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test insert_many function inserting records that already exist, with overwrite set ... ok
test_insert_many_on_existing_records_with_overwrite_true (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test insert_many function inserting records that already exist, with overwrite set ... ok
test_insert_on_existing_record_with_overwrite_false (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test insert function inserting record that already existing, with overwrite set to False. ... ok
test_insert_on_existing_record_with_overwrite_true (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test insert function inserting record that already exist, with overwrite set to True. ... ok
test_limit_and_offset_invalid_values (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test query function for invalid limit and offset values. ... ok
test_query (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test query function querying records with different query filters. ... ok
test_query_with_limit (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test query function with query filter and limit. ... ok
test_query_with_limit_and_offset (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test query function with query filter, limit and offset. ... ok
test_update (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test update function updating a record. ... ok
test_update_on_non_existing_record_with_upsert_false (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test update function on non existing record with upsert set to False. ... ok
test_update_on_non_existing_record_with_upsert_true (tests.test_datastore_json_localdrive.TestDataStoreJSONtoLocalDrive)
Test update function on non existing record with upsert set to True. ... ok
test_delete (tests.test_destination_localdrive.TestDestinationLocalDrive)
Test delete function. ... ok
test_delete_not_found (tests.test_destination_localdrive.TestDestinationLocalDrive)
Test delete function trying to delete records that does not exist. ... ok
test_exists (tests.test_destination_localdrive.TestDestinationLocalDrive)
Test exists function. ... ok
test_retrieve (tests.test_destination_localdrive.TestDestinationLocalDrive)
Test retrieve function. ... ok
test_retrieve_all (tests.test_destination_localdrive.TestDestinationLocalDrive)
Test retrieve_all function. ... ok
test_retrieve_not_found (tests.test_destination_localdrive.TestDestinationLocalDrive)
Test retrieve function retrieving record that does not exists. ... ok
test_store (tests.test_destination_localdrive.TestDestinationLocalDrive)
Test store function. ... ok
test_deserialization (tests.test_format_json.TestFormatJSON)
Test deserialization function with valid data. ... ok
test_deserialization_invalid_data (tests.test_format_json.TestFormatJSON)
Test deserialization function with invalid data. ... ok
test_serialization (tests.test_format_json.TestFormatJSON)
Test serialization function with valid data. ... ok
test_serialization_invalid_data (tests.test_format_json.TestFormatJSON)
Test serialization function with invalid data. ... ok

----------------------------------------------------------------------
Ran 29 tests in 0.014s

OK
```

# Extension

Note: No code change should be required in data store class for supporting new format or destination handlers.

## Add a new format handler
- Create a class derived from format handler abstract class (datastore.formats.FormatBase).
- Set class attribute 'NAME'.
- Declare class attribute 'CONF' map, this is required for generating configuration template for the handler.
- Implement the abstract methods defined by the abstract class.
- Update 'FORMATS' map (datastore.formats.FORMATS) to include new handler. 

## Add a new destination handler
- Create a class derived from destination handler abstract class (datastore.destinations.DestinationBase).
- Set class attribute 'NAME'.
- Declare class attribute 'CONF' map, this is required for generating configuration template for the handler.
- Implement the abstract methods defined by the abstract class.
- Update 'DESTINATIONS' map (datastore.destinations.DESTINATIONS) to include new handler. 
