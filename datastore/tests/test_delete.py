import open_data_api
import unittest

import json

import sqlalchemy
import sqlalchemy.orm as orm
import datastore.db as db

DB_CONNECTION = 'postgresql://ckanuser:pass@localhost/datastore'

import sys

import pprint

headers = {'content-type': 'application/json'}

class resource:
    def __init__(self, id):
        self.id = id

class testPackage:
    id  = 'f7a97c92-9156-46a1-9110-ae8c7b509bbc'
    name = 'annakarenina'
    resources = [resource('1234'), resource('5678')]


class TestDatastoreDelete(unittest.TestCase):
    class sysadmin_user:
        apikey = '1234'

    def setUp(self):
       self.app = open_data_api.app.test_client()
       resource = testPackage().resources[0]
       self.data = {
            'resource_id': resource.id,
            'aliases': 'books2',
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'text'}],
            'records': [{'book': 'annakarenina', 'author': 'tolstoy'},
                        {'book': 'warandpeace', 'author': 'tolstoy'}]
        }

    def tearDown(self):
        resource = testPackage().resources[0]
        data = {'resource_id': resource.id}
        postparams = json.dumps(data)
        self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)


    def _create(self):
        postparams = json.dumps(self.data)
        auth = {'Authorization': str(self.sysadmin_user.apikey)}
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'true', res_dict
        return res_dict

    def _delete(self):
        data = {'resource_id': self.data['resource_id']}
        postparams = json.dumps(data)
        auth = {'Authorization': str(self.sysadmin_user.apikey)}
        res = self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'true', res_dict
        assert res_dict['result'] == data
        return res_dict

    def test_delete_basic(self):
        self._create()
        self._delete()
        resource_id = self.data['resource_id']
        
        c = sqlalchemy.create_engine(DB_CONNECTION).connect()

        # alias should be deleted
        results = c.execute("select 1 from pg_views where viewname = '{0}'".format(self.data['aliases']))
        c.close()

        assert results.rowcount == 0

        try:
            # check that data was actually deleted: this should raise a
            # ProgrammingError as the table should not exist any more
            c = sqlalchemy.create_engine(DB_CONNECTION).connect()
            c.execute('select * from "{0}";'.format(resource_id))
            c.close()

            raise Exception("Data not deleted")
        except sqlalchemy.exc.ProgrammingError as e:
            expected_msg = 'relation "{0}" does not exist'.format(resource_id)
            assert expected_msg in str(e)


    def test_delete_invalid_resource_id(self):
        postparams = json.dumps({'resource_id': 'bad'})
        auth = {'Authorization': str(self.sysadmin_user.apikey)}
        res = self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'false'

    def test_delete_filters(self):
        self._create()
        resource_id = self.data['resource_id']

        # try and delete just the 'warandpeace' row
        data = {'resource_id': resource_id,
                'filters': {'book': 'warandpeace'}}
        postparams = json.dumps(data)
        auth = {'Authorization': str(self.sysadmin_user.apikey)}
        res = self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'true'

        
        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        result = c.execute('select * from "{0}";'.format(resource_id))
        c.close()

        results = [r for r in result]
        assert len(results) == 1
        assert results[0].book == 'annakarenina'

        # shouldn't delete anything
        data = {'resource_id': resource_id,
                'filters': {'book': 'annakarenina', 'author': 'bad'}}
        postparams = json.dumps(data)
        auth = {'Authorization': str(self.sysadmin_user.apikey)}
        res = self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'true'


        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        result = c.execute('select * from "{0}";'.format(resource_id))
        c.close()

        results = [r for r in result]
        assert len(results) == 1
        assert results[0].book == 'annakarenina'

        # delete the 'annakarenina' row
        data = {'resource_id': resource_id,
                'filters': {'book': 'annakarenina', 'author': 'tolstoy'}}
        postparams = json.dumps(data)
        auth = {'Authorization': str(self.sysadmin_user.apikey)}
        res = self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'true'

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        result = c.execute('select * from "{0}";'.format(resource_id))
        c.close()

        results = [r for r in result]
        assert len(results) == 0

        self._delete()
