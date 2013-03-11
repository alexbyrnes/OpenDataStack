OpenDataStack
=============
[![Build Status](https://travis-ci.org/alexbyrnes/OpenDataStack.png)](https://travis-ci.org/alexbyrnes/OpenDataStack)

Modular open data store, API, and Unix/Linux conventions

## Install

Install Python, virtualenv, csvkit, and PostgreSQL

    $ sudo apt-get install postgresql python-pip python-dev build-essential
    $ pip install -r requirements.txt

Set up the database:

    $ sudo -u postgres psql -c 'CREATE USER writeuser;'
    $ sudo -u postgres psql -c 'CREATE USER readonlyuser;'
    $ sudo -u postgres createdb -O writeuser datastore -E utf-8
    $ sudo -u postgres psql -c "ALTER USER writeuser WITH PASSWORD 'pass';"
    $ sudo -u postgres psql postgres -f set_permissions.sql -d datastore

Run the tests:

    $ nosetests datastore/tests/

Get some data:

    $ curl https://data.cityofchicago.org/api/views/28km-gtjn/rows.csv?accessType=DOWNLOAD | csvsql --no-constraints --insert --table firehouses --db "postgresql://writeuser:pass@localhost/datastore"

Start API server:

    $ python open_data_api.py
    
*(There is also a [tornado app] (https://github.com/alexbyrnes/OpenDataStack/blob/master/tornadoapp.py) if you want to run [tornado] (http://www.tornadoweb.org/).)*

Test the server:

    $ curl http://localhost:5000/api/action/datastore_search_sql?q=select%20*%20from%20firehouses

Result:

    [{"ENGINE": "E119", "CITY": "CHICAGO", "NAME": "E119", "ZIP": 60631, "STATE": "IL", "LOCATION": "6030 N AVONDALE AVE\nCHICAGO, IL 60631\n(41.99120656361649, -87.79879483457952)", "ADDRESS": "6030 N AVONDALE AVE"}, {"ENGINE": "E121", "CITY": "CHICAGO", "NAME": "E121", "ZIP": 60643, "STATE": "IL", "LOCATION": "1724 W 95TH ST\nCHICAGO, IL 60643\n(41.72124683806391, -87.66576328672551)", "ADDRESS": "1724 W 95TH ST"}, {"ENGINE": "E80", "CITY": "CHICAGO", "NAME": "E80", "ZIP": 60633, "STATE": "IL", "LOCATION": "12701 S DOTY AVE\nCHICAGO, IL 60633\n(41.66260010624714, -87.59059746685327)",

