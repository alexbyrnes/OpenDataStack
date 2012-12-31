#!/bin/bash
# Author:       Alex Byrnes
# Email:        alexbyrnes+github@gmail.com
# Date:         12-21-2012
# Usage:        getdata.sh [-a|--alpha] [-b=val|--beta=val]
# Description:
# Simple script for downloading data and metadata from Socrata and CKAN  
# 


# Defaults #
socrata=false

# Required program(s)
req_progs=(curl python mongoimport csvsql)
for p in ${req_progs[@]}; do
  hash "$p" 2>&- || \
  { echo >&2 " Required program \"$p\" not installed."; exit 1; }
done


dohelp()
{
  echo ""
  echo "Get Open Data"
  echo ""
  echo "Usage getdata.sh [OPTIONS]... URL"
  echo "Get data and metadata from an open data portal and store it in relational and nonrelational datastores.  Designed for use with open_data_api.py, but may be used with many frontends."
  echo ""
  echo "-s, --socrata          Pull from a Socrata data source (Default is CKAN)."
  echo "-t, --tablename        Name for table to store data in."
  echo "-mdb, --metadatastore  Name for MongoDB database (defaults to \"metadb\")."
  echo "-c, --collection       Name for MongoDB collection (defaults to \"metadata\")."
  echo "-db, --cstring         Database connection string.  Uses SQLAlchemy format."

  exit 
}

# Display usage if no parameters given
if [[ -z "$@" ]]; then
  dohelp
  exit 
fi
 
# Parse Parameters #
for ARG in $*; do
  case $ARG in
    -s|--socrata)
      socrata=true 
      ;;
   -t=*|--tablename=*)
      table=${ARG#*=}
      ;;
    -db=*|--dbconnection=*)
      dbstring=${ARG#*=}
      ;;
    -mdb=*|--metadatastore=*)
      metadatastore=${ARG#*=}
      ;;
    -c=*|--collection=*)
      metadatacollection=${ARG#*=}
      ;;
    *)
      url=$ARG
      ;;
  esac
done


if $socrata; then

  # Get data (socrata only)

  # Suffix to add to URL in each case 
  socratacsv="/rows.csv?accessType=DOWNLOAD"
  socratajson="/rows.json?accessType=DOWNLOAD"

  echo ""
  echo "Running:"
  echo "curl ${url}${socratacsv} | csvsql --no-constraints --insert --table ${table} --db \"${dbstring}\""
  curl $url$socratacsv | csvsql --no-constraints --insert --table $table --db "${dbstring}"
  echo ""
  echo ""

  # Set up options for datadownloads
  socrataoption="--socrata"

else

  socrataoption=""
  socratacsv=""
  socratajson=""

fi

# Get metadata (ckan and socrata)
echo ""
echo "Running:"
echo "curl ${url}${socratajson} | python filter_open_json.py ${socrataoption} | mongoimport -d ${metadatastore} -c ${metadatacollection}"
curl $url$socratajson | python filter_open_json.py $socrataoption | mongoimport -d $metadatastore -c $metadatacollection
echo ""
echo ""

