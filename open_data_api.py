import config
from flask import Flask, url_for
from flask import request, make_response
from flask import json
from flask import Response
from flask import jsonify
import psycopg2
import psycopg2.extras

from datetime import date

from pymongo import MongoClient
from bson import json_util

import urllib2

from datastore import ValidationError
import datastore.logic.action as datastore



import pprint

app = Flask(__name__)
app.debug = config.DEBUG
#app.config['TRAP_BAD_REQUEST_ERRORS'] = True

def add_header(data):
    response = make_response(data)
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response



@app.route('/api/action/datastore_upsert', methods = ['GET', 'POST'])
def datastore_upsert():

    request_json = json.loads(request.data)

    if all(s in request_json for s in ('resource_id', 'method', 'records')):
        try:
            function = datastore.datastore_upsert
        
            context = {}
            data_dict = request_json
            data_dict['connection_url'] = config.DB_CONNECTION
            reply = {}
            reply['help'] = function.__doc__
            reply['success'] = 'true'

            reply['result'] = function(context, data_dict)
        except ValidationError as e:
            return api_error(e)
        except Exception as e:
            return api_error(e)
        return add_header(json.dumps(reply))
    else:
        return not_found()


@app.route('/api/action/datastore_delete', methods = ['GET', 'POST'])
def datastore_delete():

    request_json = json.loads(request.data)

    if 'resource_id' in request_json:
        try: 
            function = datastore.datastore_delete
        
            context = {}
            data_dict = request_json
            data_dict['connection_url'] = config.DB_CONNECTION
            reply = {}
            reply['help'] = function.__doc__
            reply['success'] = 'true'
            reply['result'] = function(context, data_dict)
        except ValidationError as e:
            return api_error(e)
        except Exception as e:
            return api_error(e)

        return add_header(json.dumps(reply))
    else:
        return not_found()


@app.route('/api/action/datastore_create', methods = ['GET', 'POST'])
def datastore_create():

    request_json = json.loads(request.data)

    if 'resource_id' in request_json and ('fields' in request_json or 'records' in request_json):

        try:
            function = datastore.datastore_create
        
            context = {}
            data_dict = request_json
            data_dict['connection_url'] = config.DB_CONNECTION
            reply = {}
            reply['help'] = function.__doc__
            reply['success'] = 'true'
    
            reply['result'] = function(context, data_dict)

            return add_header(json.dumps(reply))
        except ValidationError as e:
            return api_error(e)
        except Exception as e:
            return api_error(e) 
    else:
        return not_found()


@app.route('/api/action/datastore_search_sql', methods = ['GET', 'POST'])
def datastore_search_sql():

    request_json = json.loads(request.data)

    if 'sql' in request_json:
        try:
            function = datastore.datastore_search_sql
        
            context = {}
            data_dict = request_json
            data_dict['connection_url'] = config.DB_CONNECTION
            reply = {}
            reply['help'] = function.__doc__
            reply['success'] = 'true'
    
            reply['result'] = function(context, data_dict)

        except ValidationError as e:
            return api_error(e)
        except Exception as e:
            return api_error(e)

        return add_header(json.dumps(reply))
    else:
        return not_found()


@app.route('/api/action/datastore_search', methods = ['GET', 'POST'])
def datastore_search():
    request_json = json.loads(request.data)

    if 'resource_id' in request_json:
        try:
            function = datastore.datastore_search

            context = {}
            data_dict = request_json
            data_dict['connection_url'] = config.DB_CONNECTION
            reply = {}
            reply['help'] = function.__doc__
            reply['success'] = 'true'
    
            reply['result'] = function(context, data_dict)

            return add_header(json.dumps(reply))
        except ValidationError as e:
            return api_error(e)
        except Exception as e:
            return api_error(e)
    else:
        return not_found()


@app.route('/api/action/package_search', methods = ['GET', 'POST'])
def package_search():

    if 'q' in request.args:

        query = request.args['q']
        
        f = urllib2.urlopen('http://localhost:8983/solr/select?wt=json&q=' + query)

	reply = f.read()

        return add_header(reply)
    else:
        return not_found()






@app.route('/api/action/package_show', methods = ['GET', 'POST'])
def package_show():
    
    connection = MongoClient() 
    db = connection[config.METADATA_DB]
    collection = db[config.METADATA_COLLECTION]

    if 'id' in request.args:
        package_id = request.args['id']
        reply = collection.find_one({"id": package_id})
        return add_header(json.dumps(reply, default=json_util.default))
    else:
        return not_found()

# Used for encoding Postgres dates
class DateEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, date):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def api_error(error=None):
    message = {
            'success': 'false',
            'message': str(error)
            }
    resp = jsonify(message)
    resp.status_code = 404
    return resp


def not_found(error=None):
    message = {
            'success': 'false',
            'status': 404,
            'message': 'Not Found: ' + request.url,
            }
    resp = jsonify(message)
    resp.status_code = 404
    return resp


if __name__ == '__main__':
    app.run(config.SERVER_NAME, config.SERVER_PORT)

