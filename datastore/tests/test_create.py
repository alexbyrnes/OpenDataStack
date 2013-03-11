import open_data_api
import unittest

import json

import sqlalchemy
import sqlalchemy.orm as orm
import datastore.db as db

DB_CONNECTION = 'postgresql://writeuser:pass@localhost/datastore'

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


class TestDatastoreCreate(unittest.TestCase):

    class sysadmin_user:
        apikey = '1234'

    def setUp(self):
        self.app = open_data_api.app.test_client()

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
    def test_create_requires_auth(self):
        resource = testPackage().resources[0]
        data = {
            'resource_id': resource.id
        }
        postparams = json.dumps(data)
        res = self.app.post('/api/action/datastore_create', params=postparams,
                            status=403)
        res_dict = json.loads(res.data)
        assert res_dict['success']== 'false'
    '''

    def test_create_empty_fails(self):
        data = {}
        postparams = json.dumps(data)
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'false'

    def test_create_invalid_alias_name(self):
        resource = testPackage().resources[0]

        data = {
            'resource_id': resource.id,
            'aliases': u'foo"bar',
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'text'}]
        }
        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'false'

        data = {
            'resource_id': resource.id,
            'aliases': u'fo%25bar',
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'text'}]
        }

        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false'

    def test_create_invalid_field_type(self):
        resource = testPackage().resources[0]
        data = {
            'resource_id': resource.id,
            'fields': [{'id': 'book', 'type': 'int['},  # this is invalid
                       {'id': 'author', 'type': 'INVALID'}]
        }
        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'false'

    def test_create_invalid_field_name(self):
        resource = testPackage().resources[0]
        data = {
            'resource_id': resource.id,
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': '_author', 'type': 'text'}]
        }
        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'false'

        data = {
            'resource_id': resource.id,
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': '"author', 'type': 'text'}]
        }
        postparams = json.dumps(data)
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'false'

        data = {
            'resource_id': resource.id,
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': '', 'type': 'text'}]
        }
        postparams = json.dumps(data)
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'false'

    def test_create_invalid_record_field(self):
        resource = testPackage().resources[0]
        data = {
            'resource_id': resource.id,
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'text'}],
            'records': [{'book': 'annakarenina', 'author': 'tolstoy'},
                        {'book': 'warandpeace', 'published': '1869'}]
        }
        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'false'

    def test_bad_records(self):
        resource = testPackage().resources[0]
        data = {
            'resource_id': resource.id,
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'text'}],
            'records': ['bad']  # treat author as null
        }
        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false'

        resource = testPackage().resources[0]
        data = {
            'resource_id': resource.id,
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'text'}],
            'records': [{'book': 'annakarenina', 'author': 'tolstoy'},
                        [],
                        {'book': 'warandpeace'}]  # treat author as null
        }
        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false'

    def test_create_invalid_index(self):
        resource = testPackage().resources[0]
        data = {
            'resource_id': resource.id,
            'indexes': 'book, dummy',
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'text'}]
        }
        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'false'

    def test_create_invalid_unique_index(self):
        resource = testPackage().resources[0]
        data = {
            'resource_id': resource.id,
            'primary_key': 'dummy',
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'text'}]
        }
        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)
        assert res_dict['success'] == 'false'

    def test_create_alias_twice(self):
        resource = testPackage().resources[1]
        
        data = {
            'resource_id': resource.id,
            'aliases': 'new_alias2',
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'text'}]
        }

        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)

        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true', res_dict

        resource = testPackage().resources[0]

        data = {
            'resource_id': resource.id,
            'aliases': 'new_alias2',
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'text'}]
        }

        postparams = json.dumps(data)

        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)

        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false', res_dict

    def test_create_basic(self):
        resource = testPackage().resources[0]
        aliases = [u'great_list_of_books', u'another_list_of_b\xfcks']
        data = {
            'resource_id': resource.id,
            'aliases': aliases,
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'json'}],
            'indexes': [['book', 'author'], 'author'],
            'records': [
                        {'book': 'crime', 'author': ['tolstoy', 'dostoevsky']},
                        {'book': 'annakarenina', 'author': ['tolstoy', 'putin']},
                        {'book': 'warandpeace'}]  # treat author as null
        }
        ### Firstly test to see if resource things it has datastore table
        #postparams = '%s=1' % json.dumps({'id': resource.id})
        #
        #res = self.app.post('/api/action/resource_show', params=postparams,
        #                    extra_environ=auth)
        #res_dict = json.loads(res.data)
        #assert res_dict['result']['datastore_active'] == False

        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true'
        res = res_dict['result']
        assert res['resource_id'] == data['resource_id']
        assert res['fields'] == data['fields'], res['fields']
        assert res['records'] == data['records']

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select * from "{0}"'.format(resource.id))
        c.close()

        assert results.rowcount == 3
        for i, row in enumerate(results):
            assert data['records'][i].get('book') == row['book']
            assert data['records'][i].get('author') == (
                json.loads(row['author'][0]) if row['author'] else None)

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('''
            select * from "{0}" where _full_text @@ to_tsquery('warandpeace')
            '''.format(resource.id))
        c.close()

        assert results.rowcount == 1, results.rowcount

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('''
            select * from "{0}" where _full_text @@ to_tsquery('tolstoy')
            '''.format(resource.id))
        c.close()

        assert results.rowcount == 2

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        # check aliases for resource
        for alias in aliases:

            results = [row for row in c.execute(u'select * from "{0}"'.format(resource.id))]
            results_alias = [row for row in c.execute(u'select * from "{0}"'.format(alias))]

            assert results == results_alias

            sql = (u"select * from _table_metadata "
                "where alias_of='{0}' and name='{1}'").format(resource.id, alias)

            results = c.execute(sql)
            assert results.rowcount == 1

        c.close()

        # check to test to see if resource now has a datastore table
        #postparams = '%s=1' % json.dumps({'id': resource.id})
        #
        #res = self.app.post('/api/action/resource_show', data=postparams, headers=headers)
        #res_dict = json.loads(res.data)
        #assert res_dict['result']['datastore_active'] == True

        #######  insert again simple
        data2 = {
            'resource_id': resource.id,
            'records': [{'book': 'hagji murat', 'author': ['tolstoy']}]
        }

        postparams = json.dumps(data2)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true', res_dict

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select * from "{0}"'.format(resource.id))
        c.close()

        assert results.rowcount == 4

        all_data = data['records'] + data2['records']
        for i, row in enumerate(results):
            assert all_data[i].get('book') == row['book']
            assert all_data[i].get('author') == (
                json.loads(row['author'][0]) if row['author'] else None)

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('''
            select * from "{0}" where _full_text @@ 'tolstoy'
            '''.format(resource.id))
        c.close()
 
        assert results.rowcount == 3

        #######  insert again extra field
        data3 = {
            'resource_id': resource.id,
            'records': [{'book': 'crime and punsihment',
                         'author': ['dostoevsky'], 'rating': 2}],
            'indexes': ['rating']
        }

        postparams = json.dumps(data3)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true', res_dict

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('select * from "{0}"'.format(resource.id))
        c.close()

        assert results.rowcount == 5

        all_data = data['records'] + data2['records'] + data3['records']
        for i, row in enumerate(results):
            assert all_data[i].get('book') == row['book'], (i, all_data[i].get('book'), row['book'])
            assert all_data[i].get('author') == (json.loads(row['author'][0]) if row['author'] else None)

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('''select * from "{0}" where _full_text @@ to_tsquery('dostoevsky') '''.format(resource.id))
        c.close()

        assert results.rowcount == 2

        #######  insert again which will fail because of unique book name
        data4 = {
            'resource_id': resource.id,
            'records': [{'book': 'warandpeace'}],
            'primary_key': 'book'
        }

        postparams = json.dumps(data4)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false'
        assert 'constraints' in res_dict['message'], res_dict

        #######  insert again which should not fail because constraint is removed
        data5 = {
            'resource_id': resource.id,
            'aliases': 'another_alias',  # replaces aliases
            'records': [{'book': 'warandpeace'}],
            'primary_key': ''
        }

        postparams = json.dumps(data5)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success']== 'true', res_dict

        # new aliases should replace old aliases
        for alias in aliases:
            sql = (u"select * from _table_metadata "
                "where alias_of='{0}' and name='{1}'").format(resource.id, alias)

            c = sqlalchemy.create_engine(DB_CONNECTION).connect()
            results = c.execute(sql)
            c.close()

            assert results.rowcount == 0

        sql = (u"select * from _table_metadata "
            "where alias_of='{0}' and name='{1}'").format(resource.id, 'another_alias')
        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute(sql)
        c.close()

        assert results.rowcount == 1

        #######  insert array type
        data6 = {
            'resource_id': resource.id,
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'json'},
                       {'id': 'rating', 'type': 'int'},
                       {'id': 'characters', 'type': '_text'}],  # this is an array of strings
            'records': [{'book': 'the hobbit',
                         'author': ['tolkien'], 'characters': ['Bilbo', 'Gandalf']}],
            'indexes': ['characters']
        }

        postparams = json.dumps(data6)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true', res_dict

        #######  insert type that requires additional lookup
        data7 = {
            'resource_id': resource.id,
            'fields': [{'id': 'book', 'type': 'text'},
                       {'id': 'author', 'type': 'json'},
                       {'id': 'rating', 'type': 'int'},
                       {'id': 'characters', 'type': '_text'},
                       {'id': 'location', 'type': 'int[2]'}],
            'records': [{'book': 'lord of the rings',
                         'author': ['tolkien'], 'location': [3, -42]}],
            'indexes': ['characters']
        }

        postparams = json.dumps(data7)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'true', res_dict

    #def test_guess_types(self):
    def guess_types(self):
        resource = testPackage().resources[1]

        data = {
            'resource_id': resource.id
        }
        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_delete', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        data = {
            'resource_id': resource.id,
            'fields': [{'id': 'author', 'type': 'json'},
                       {'id': 'count'},
                       {'id': 'book'},
                       {'id': 'date'}],
            'records': [{'book': 'annakarenina', 'author': 'tolstoy',
                         'count': 1, 'date': '2005-12-01', 'count2': 0.5},
                        {'book': 'crime', 'author': ['tolstoy', 'dostoevsky']},
                        {'book': 'warandpeace'}]  # treat author as null
        }
        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('''select * from "{0}" '''.format(resource.id))
        c.close()

        types = [db._pg_types[field[1]] for field in results.cursor.description]

        assert types == [u'int4', u'tsvector', u'nested', u'int4', u'text', u'timestamp', u'float8'], types

        assert results.rowcount == 3
        for i, row in enumerate(results):
            assert data['records'][i].get('book') == row['book']
            assert data['records'][i].get('author') == (
                json.loads(row['author'][0]) if row['author'] else None)

        ### extend types

        data = {
            'resource_id': resource.id,
            'fields': [{'id': 'author', 'type': 'text'},
                       {'id': 'count'},
                       {'id': 'book'},
                       {'id': 'date'},
                       {'id': 'count2'},
                       {'id': 'extra', 'type':'text'},
                       {'id': 'date2'},
                      ],
            'records': [{'book': 'annakarenina', 'author': 'tolstoy',
                         'count': 1, 'date': '2005-12-01', 'count2': 2,
                         'nested': [1, 2], 'date2': '2005-12-01'}]
        }

        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        c = sqlalchemy.create_engine(DB_CONNECTION).connect()
        results = c.execute('''select * from "{0}" '''.format(resource.id))
        c.close()

        types = [db._pg_types[field[1]] for field in results.cursor.description]

        assert types == [u'int4',  # id
                         u'tsvector',  # fulltext
                         u'nested',  # author
                         u'int4',  # count
                         u'text',  # book
                         u'timestamp',  # date
                         u'float8',  # count2
                         u'text',  # extra
                         u'timestamp',  # date2
                         u'nested',  # count3
                        ], types

        ### fields resupplied in wrong order

        data = {
            'resource_id': resource.id,
            'fields': [{'id': 'author', 'type': 'text'},
                       {'id': 'count'},
                       {'id': 'date'},  # date and book in wrong order
                       {'id': 'book'},
                       {'id': 'count2'},
                       {'id': 'extra', 'type':'text'},
                       {'id': 'date2'},
                      ],
            'records': [{'book': 'annakarenina', 'author': 'tolstoy',
                         'count': 1, 'date': '2005-12-01', 'count2': 2,
                         'count3': 432, 'date2': '2005-12-01'}]
        }

        postparams = json.dumps(data)
        
        res = self.app.post('/api/action/datastore_create', data=postparams, headers=headers)
        res_dict = json.loads(res.data)

        assert res_dict['success'] == 'false'

