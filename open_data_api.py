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

app = Flask(__name__)
app.debug = config.DEBUG


def add_header(data):
    response = make_response(data)
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route('/api/action/datastore_search_sql', methods = ['GET', 'POST'])
def datastore_search_sql():

    conn = psycopg2.connect(config.DB_CONNECTION)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if 'q' in request.args:

        query = request.args['q']

        cur.execute(query)
	data = cur.fetchall()
        reply = []

        for row in data:
            reply.append(dict(row))
       
        cur.close()
        return add_header(json.dumps(reply))
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


def not_found(error=None):
    message = {
            'status': 404,
            'message': 'Not Found: ' + request.url,
            }
    resp = jsonify(message)
    resp.status_code = 404
    return resp


if __name__ == '__main__':
    app.run(config.SERVER_NAME, config.SERVER_PORT)

