/*
This script sets-up the permissions for the the datastore.

creates a new datastore database and
a new read-only user for ckan who will only be able
to select from the datastore database but has no create/write/edit
permission or any permissions on other databases.

Please set the variables to you current set-up. For testing purposes it
is possible to set maindb = datastoredb.

To run the script, execute:
    sudo -u postgres psql postgres -f set_permissions.sql
*/


-- the name of the datastore database
\set datastoredb "datastore"
-- username of the datastore user that can write
\set wuser "writeuser"
-- username of the datastore user who has only read permissions
\set rouser "readonlyuser"

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE USAGE ON SCHEMA public FROM PUBLIC;

GRANT CREATE ON SCHEMA public TO :wuser;
GRANT USAGE ON SCHEMA public TO :wuser;

-- grant select permissions for read-only user
GRANT CONNECT ON DATABASE :datastoredb TO :rouser;
GRANT USAGE ON SCHEMA public TO :rouser;

-- grant access to current tables and views to read-only user
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :rouser;

-- grant access to new tables and views by default
---- the permissions will be set when the write user creates a table
ALTER DEFAULT PRIVILEGES FOR USER :wuser IN SCHEMA public
   GRANT SELECT ON TABLES TO :rouser;


CREATE OR REPLACE VIEW "_table_metadata" AS 
 SELECT DISTINCT
    substr(md5(dependee.relname || COALESCE(dependent.relname, '')), 0, 17) AS "_id",
    dependee.relname AS name,
    dependee.oid AS oid,
    dependent.relname AS alias_of
    -- dependent.oid AS oid
 FROM
    pg_class AS dependee
    LEFT OUTER JOIN pg_rewrite AS r ON r.ev_class = dependee.oid
    LEFT OUTER JOIN pg_depend AS d ON d.objid = r.oid
    LEFT OUTER JOIN pg_class AS dependent ON d.refobjid = dependent.oid
 WHERE
    (dependee.oid != dependent.oid OR dependent.oid IS NULL) AND
    (dependee.relname IN (SELECT tablename FROM pg_catalog.pg_tables)
    OR dependee.relname IN (SELECT viewname FROM pg_catalog.pg_views)) AND
    dependee.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname='public')
    ORDER BY dependee.oid DESC;
  


