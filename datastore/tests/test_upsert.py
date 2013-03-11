import open_data_api
import unittest

import json
import datetime

import sqlalchemy
import sqlalchemy.orm as orm

import datastore.db as db

headers = {'content-type': 'application/json'}

DB_CONNECTION = 'postgresql://writeuser:pass@localhost/datastore'


class resource:
    def __init__(self, id):
        self.id = id

class testPackage:
    id  = 'f7a97c92-9156-46a1-9110-ae8c7b509bbc'
    name = 'annakarenina'
    resources = [resource('1234'), resource('5678')]



class TestDatastoreUpsert(unittest.TestCase):

    def setUp(self):
        self.app = open_data_api.app.test_client()
        resource = testPackage().resources[0]
        self.data = {
            'resource_id': resource.id,
            'fields': [{'id': u'b\xfck', 'type': 'text'},
                       {'id': 'author', 'type': 'text'},
                       {'id': 'nested', 'type': 'json'},
                       {'id': 'characters', 'type': 'text[]'},
                       {'id': 'published'}],
            'primary_key': u'b\xfck',
            'records': [{u'b\xfck': 'annakarenina', 'author': 'tolstoy',
                        'published': '2005-03-01', 'nested': ['b', {'moo': 'moo'}]},
                        {u'b\xfck': 'warandpeace', 'author': 'tolstoy',
                        'nested': {'a':'b'}}
                       ]
            }
        postparams = json.dumps(self.data)

        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'true'


    def tearDown(self):
        resource = testPackage().resources[0]
        data = {'resource_id': resource.id}
        postparams = json.dumps(data)
        self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)

        resource = testPackage().resources[1]
        data = {'resource_id': resource.id}
        postparams = json.dumps(data)
        self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)

    '''	
    def test_upsert_requires_auth(self):
        data = {
            'resource_id': self.data['resource_id']
        }
        postparams = json.dumps(data)
        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'false'
    '''
	
    def test_upsert_empty_fails(self):
        postparams = json.dumps({})

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'false'

    def test_upsert_basic(self):
        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select 1 from "{0}"'.format(self.data['resource_id']))
        assert results.rowcount == 2
        c.close()
		
        hhguide = u"hitchhiker's guide to the galaxy"

        data = {
            'resource_id': self.data['resource_id'],
            'method': 'upsert',
            'records': [{
                'author': 'adams',
                'nested': {'a': 2, 'b': {'c': 'd'}},
                'characters': ['Arthur Dent', 'Marvin'],
                'nested': {'foo': 'bar'},
                u'b\xfck': hhguide}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true'

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select * from "{0}"'.format(self.data['resource_id']))
        assert results.rowcount == 3

        records = results.fetchall()
        assert records[2][u'b\xfck'] == hhguide
        assert records[2].author == 'adams'
        assert records[2].characters == ['Arthur Dent', 'Marvin']
        assert json.loads(records[2].nested.json) == {'foo': 'bar'}
        c.close()

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute("select * from \"{0}\" where author='{1}'".format(self.data['resource_id'], 'adams'))
        assert results.rowcount == 1
        c.close()

        # upsert only the publish date
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'upsert',
            'records': [{'published': '1979-1-1', u'b\xfck': hhguide}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true'

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select * from "{0}"'.format(self.data['resource_id']))
        assert results.rowcount == 3

        records = results.fetchall()
        assert records[2][u'b\xfck'] == hhguide
        assert records[2].author == 'adams'
        assert records[2].published == datetime.datetime(1979, 1, 1)
        c.close()

        # delete publish date
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'upsert',
            'records': [{u'b\xfck': hhguide, 'published': None}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true'

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select * from "{0}"'.format(self.data['resource_id']))
        assert results.rowcount == 3

        records = results.fetchall()
        assert records[2][u'b\xfck'] == hhguide
        assert records[2].author == 'adams'
        assert records[2].published == None
        c.close()

        data = {
            'resource_id': self.data['resource_id'],
            'method': 'upsert',
            'records': [{'author': 'tolkien', u'b\xfck': 'the hobbit'}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true'

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select * from "{0}"'.format(self.data['resource_id']))
        assert results.rowcount == 4

        records = results.fetchall()
        assert records[3][u'b\xfck'] == 'the hobbit'
        assert records[3].author == 'tolkien'
        c.close()

        # test % in records
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'upsert',
            'records': [{'author': 'tol % kien', u'b\xfck': 'the % hobbit'}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true'

    def test_upsert_missing_key(self):
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'upsert',
            'records': [{'author': 'tolkien'}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false'

    def test_upsert_non_existing_field(self):
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'upsert',
            'records': [{u'b\xfck': 'annakarenina', 'dummy': 'tolkien'}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false'


class TestDatastoreInsert(unittest.TestCase):

    def setUp(self):
        self.app = open_data_api.app.test_client()
        resource = testPackage().resources[0]
        self.data = {
            'resource_id': resource.id,
            'fields': [{'id': u'b\xfck', 'type': 'text'},
                       {'id': 'author', 'type': 'text'},
                       {'id': 'nested', 'type': 'json'},
                       {'id': 'characters', 'type': 'text[]'},
                       {'id': 'published'}],
            'primary_key': u'b\xfck',
            'records': [{u'b\xfck': 'annakarenina', 'author': 'tolstoy',
                        'published': '2005-03-01', 'nested': ['b', {'moo': 'moo'}]},
                        {u'b\xfck': 'warandpeace', 'author': 'tolstoy',
                        'nested': {'a':'b'}}
                       ]
            }
        postparams = json.dumps(self.data)

        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'true'

    def tearDown(self):
        resource = testPackage().resources[0]
        data = {'resource_id': resource.id}
        postparams = json.dumps(data)
        self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)

        resource = testPackage().resources[1]
        data = {'resource_id': resource.id}
        postparams = json.dumps(data)
        self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)

		
    def test_insert_non_existing_field(self):
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'insert',
            'records': [{u'b\xfck': 'the hobbit', 'dummy': 'tolkien'}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false'

    def test_insert_with_index_violation(self):
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'insert',
            'records': [{u'b\xfck': 'annakarenina'}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false'

    def test_insert_basic(self):
        hhguide = u"hitchhiker's guide to the galaxy"
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'insert',
            'records': [{
                'author': 'adams',
                'characters': ['Arthur Dent', 'Marvin'],
                'nested': {'foo': 'bar', 'baz': 3},
                u'b\xfck': hhguide}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true'

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select * from "{0}"'.format(self.data['resource_id']))
        c.close()

        assert results.rowcount == 3


class TestDatastoreUpdate(unittest.TestCase):

    def setUp(self):
        self.app = open_data_api.app.test_client()
        resource = testPackage().resources[0]
        hhguide = u"hitchhiker's guide to the galaxy"
        self.data = {
            'resource_id': resource.id,
            'fields': [{'id': u'b\xfck', 'type': 'text'},
                       {'id': 'author', 'type': 'text'},
                       {'id': 'nested', 'type': 'json'},
                       {'id': 'characters', 'type': 'text[]'},
                       {'id': 'published'}],
            'primary_key': u'b\xfck',
            'records': [{u'b\xfck': 'annakarenina', 'author': 'tolstoy',
                        'published': '2005-03-01', 'nested': ['b', {'moo': 'moo'}]},
                        {u'b\xfck': 'warandpeace', 'author': 'tolstoy',
                        'nested': {'a':'b'}},
                        {'author': 'adams',
                        'characters': ['Arthur Dent', 'Marvin'],
                        'nested': {'foo': 'bar'},
                        u'b\xfck': hhguide}
                       ]
            }
        postparams = json.dumps(self.data)

        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'true'

    def tearDown(self):
        resource = testPackage().resources[0]
        data = {'resource_id': resource.id}
        postparams = json.dumps(data)
        self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)

        resource = testPackage().resources[1]
        data = {'resource_id': resource.id}
        postparams = json.dumps(data)
        self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)
	

    def test_update_basic(self):
        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select 1 from "{0}"'.format(self.data['resource_id']))
        assert results.rowcount == 3, results.rowcount
        c.close()

        hhguide = u"hitchhiker's guide to the galaxy"
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'update',
            'records': [{
                'author': 'adams',
                'characters': ['Arthur Dent', 'Marvin'],
                'nested': {'baz': 3},
                u'b\xfck': hhguide}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true'

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select * from "{0}"'.format(self.data['resource_id']))
        assert results.rowcount == 3

        records = results.fetchall()
        assert records[2][u'b\xfck'] == hhguide
        assert records[2].author == 'adams'
        c.close()

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute("select * from \"{0}\" where author='{1}'".format(self.data['resource_id'], 'adams'))
        assert results.rowcount == 1
        c.close()

        # update only the publish date
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'update',
            'records': [{'published': '1979-1-1', u'b\xfck': hhguide}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true'

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select * from "{0}"'.format(self.data['resource_id']))
        c.close()
        assert results.rowcount == 3

        records = results.fetchall()
        assert records[2][u'b\xfck'] == hhguide
        assert records[2].author == 'adams'
        assert records[2].published == datetime.datetime(1979, 1, 1)

        # delete publish date
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'update',
            'records': [{u'b\xfck': hhguide, 'published': None}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true'

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select * from "{0}"'.format(self.data['resource_id']))
        c.close()
        assert results.rowcount == 3

        records = results.fetchall()
        assert records[2][u'b\xfck'] == hhguide
        assert records[2].author == 'adams'
        assert records[2].published == None

    def test_update_missing_key(self):
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'update',
            'records': [{'author': 'tolkien'}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false'

    def test_update_non_existing_key(self):
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'update',
            'records': [{u'b\xfck': '', 'author': 'tolkien'}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false'

    def test_update_non_existing_field(self):
        data = {
            'resource_id': self.data['resource_id'],
            'method': 'update',
            'records': [{u'b\xfck': 'annakarenina', 'dummy': 'tolkien'}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_upsert', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false'

