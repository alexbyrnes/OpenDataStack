OpenDataStack
=============

Modular open data store, API, and Unix/Linux conventions

Install python, virtualenv, csvkit, postgres, and mongodb:

    sudo apt-get install 


Get Socrata data:

    curl https://data.cityofchicago.org/api/views/28km-gtjn/rows.csv?accessType=DOWNLOAD | csvsql --no-constraints --insert --table firehouses --db "postgresql://odsuser:odspass@localhost/opendatastore"

Get Socrata metadata:

    curl https://data.cityofchicago.org/api/views/28km-gtjn/rows.json?accessType=DOWNLOAD | python filter_open_json.py --socrata | mongoimport -d metadb -c metadata

CKAN metadata:

    curl http://opendata.cmap.illinois.gov/api/action/package_show?id=4cb2a4e2-8aaa-484d-8a9f-0874e70697fe | python filter_open_json.py | mongoimport -d metadb -c metadata



# getdata.sh

The getdata.sh script consolidates the options above, checks that the right programs are installed, has help.  If you need to pass more options to csvsql or mongoimport, it's better to use the individual commands.

Get CKAN metadata:

    ./getdata.sh -mdb=metadb -c=metadata http://opendata.cmap.illinois.gov/api/action/package_show?id=4cb2a4e2-8aaa-484d-8a9f-0874e70697fe

Get Socrata data and metadata (notice shortened URL so it works for both):

    ./getdata.sh -s -t=firehouses -mdb=metadb -c=metadata -db=postgresql://odsuser:odspass@localhost/opendatastore https://data.cityofchicago.org/api/views/28km-gtjn


It's a good idea to revoke privileges other than SELECT for the user in the connection string when serving through a public API (example for Postgres):

    REVOKE ALL PRIVILEGES ON firehouses FROM odsuser;
    GRANT SELECT ON firehouses TO odsuser;

Start API server:

    python open_data_api.py

Get data:

    curl http://localhost:5000/api/action/datastore_search_sql?q=select%20*%20from%20firehouses

Result:

    [{"ENGINE": "E119", "CITY": "CHICAGO", "NAME": "E119", "ZIP": 60631, "STATE": "IL", "LOCATION": "6030 N AVONDALE AVE\nCHICAGO, IL 60631\n(41.99120656361649, -87.79879483457952)", "ADDRESS": "6030 N AVONDALE AVE"}, {"ENGINE": "E121", "CITY": "CHICAGO", "NAME": "E121", "ZIP": 60643, "STATE": "IL", "LOCATION": "1724 W 95TH ST\nCHICAGO, IL 60643\n(41.72124683806391, -87.66576328672551)", "ADDRESS": "1724 W 95TH ST"}, {"ENGINE": "E80", "CITY": "CHICAGO", "NAME": "E80", "ZIP": 60633, "STATE": "IL", "LOCATION": "12701 S DOTY AVE\nCHICAGO, IL 60633\n(41.66260010624714, -87.59059746685327)",



# The payoff!  

* [sample_interface.html] () A hello world app.  Gets metadata and a data table.

* pico-ckan. A lightweight CKAN clone.

* D3, highcharts, jQuery...  Anything that reads JSON.


