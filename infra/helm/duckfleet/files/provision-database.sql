-- Administrator-only provisioning. No runtime or migration container runs this.
-- Inputs arrive through environment variables, never command-line passwords.
\set ON_ERROR_STOP on
\getenv migrator DUCKFLEET_DB_MIGRATOR_ROLE
\getenv gateway DUCKFLEET_DB_GATEWAY_ROLE
\getenv worker DUCKFLEET_DB_WORKER_ROLE
\getenv maintainer DUCKFLEET_DB_MAINTAINER_ROLE
\getenv catalog DUCKFLEET_DB_CATALOG_ROLE
\getenv migrator_password DUCKFLEET_DB_MIGRATOR_PASSWORD
\getenv gateway_password DUCKFLEET_DB_GATEWAY_PASSWORD
\getenv worker_password DUCKFLEET_DB_WORKER_PASSWORD
\getenv maintainer_password DUCKFLEET_DB_MAINTAINER_PASSWORD
\getenv catalog_password DUCKFLEET_DB_CATALOG_PASSWORD
\getenv platform_db DUCKFLEET_PLATFORM_DATABASE
\getenv lake_db DUCKFLEET_LAKE_DATABASE
\getenv adopt DUCKFLEET_ADOPT_DATABASE_OWNER

SELECT :'platform_db'<>:'lake_db'
    AND :'platform_db' ~ '^[a-z_][a-z0-9_]{0,62}$'
    AND :'lake_db' ~ '^[a-z_][a-z0-9_]{0,62}$' AS valid_databases \gset
\if :valid_databases
\else
    \echo 'Dedicated platform and lake database names must be distinct lowercase identifiers'
    DO $refused$ BEGIN RAISE EXCEPTION 'database provisioning refused; see preceding diagnostic'; END $refused$;
\endif
BEGIN;
SELECT count(*)=5 AND count(DISTINCT name)=5 AND count(DISTINCT password)=5
    AND bool_and(name ~ '^[a-z_][a-z0-9_]{0,62}$' AND length(password)>=16) AS valid_inputs
FROM (VALUES (:'migrator',:'migrator_password'),(:'gateway',:'gateway_password'),
    (:'worker',:'worker_password'),(:'maintainer',:'maintainer_password'),(:'catalog',:'catalog_password')) v(name,password) \gset
\if :valid_inputs
\else
    \echo 'Refusing invalid/distinctness/password inputs for database roles'
    DO $refused$ BEGIN RAISE EXCEPTION 'database provisioning refused; see preceding diagnostic'; END $refused$;
\endif
SELECT NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname IN
    (:'migrator',:'gateway',:'worker',:'maintainer',:'catalog') AND
    (rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls
     OR EXISTS(SELECT 1 FROM pg_auth_members WHERE member=pg_roles.oid))) AS safe_existing_roles \gset
\if :safe_existing_roles
\else
    \echo 'Existing component roles have elevated privileges or memberships; inspect them before provisioning'
    DO $refused$ BEGIN RAISE EXCEPTION 'database provisioning refused; see preceding diagnostic'; END $refused$;
\endif
SELECT format('CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', name)
FROM (VALUES (:'migrator'),(:'gateway'),(:'worker'),(:'maintainer'),(:'catalog')) v(name)
WHERE NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=name) \gexec
SELECT format('ALTER ROLE %I LOGIN PASSWORD %L', name, password)
FROM (VALUES (:'migrator',:'migrator_password'),(:'gateway',:'gateway_password'),
    (:'worker',:'worker_password'),(:'maintainer',:'maintainer_password'),(:'catalog',:'catalog_password')) v(name,password) \gexec
COMMIT;
SELECT format('CREATE DATABASE %I OWNER %I', :'platform_db', :'migrator')
WHERE NOT EXISTS(SELECT 1 FROM pg_database WHERE datname=:'platform_db') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'lake_db', :'catalog')
WHERE NOT EXISTS(SELECT 1 FROM pg_database WHERE datname=:'lake_db') \gexec

\connect :platform_db
SELECT pg_get_userbyid(datdba)=:'migrator' AS owner_matches FROM pg_database WHERE datname=current_database() \gset
\if :owner_matches
\else
    \if :adopt
    \else
        \echo 'Existing platform owner differs: stop compute, review the dedicated database, then explicitly enable DUCKFLEET_ADOPT_DATABASE_OWNER=yes'
        DO $refused$ BEGIN RAISE EXCEPTION 'database provisioning refused; see preceding diagnostic'; END $refused$;
    \endif
\endif
BEGIN;
ALTER DATABASE :"platform_db" OWNER TO :"migrator";
REVOKE ALL ON DATABASE :"platform_db" FROM PUBLIC,:"catalog";
GRANT CONNECT ON DATABASE :"platform_db" TO :"gateway",:"worker",:"maintainer";
-- System SQL can aggregate only bounded sources; bound external-sort spill too.
-- This setting requires the administrator and cannot be raised by the gateway.
ALTER ROLE :"gateway" IN DATABASE :"platform_db" SET temp_file_limit = '16MB';
REVOKE ALL ON SCHEMA public FROM PUBLIC;
ALTER SCHEMA public OWNER TO :"migrator";
-- Transfer only objects in this dedicated database/schema. REASSIGN OWNED is
-- deliberately avoided: it also changes ownership of shared cluster objects.
SELECT format('ALTER %s public.%I OWNER TO %I', CASE relkind WHEN 'S' THEN 'SEQUENCE' WHEN 'v' THEN 'VIEW'
    WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE 'TABLE' END, relname, :'migrator')
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='public' AND relkind IN ('r','p','S','v','m')
  AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.deptype='e') \gexec
SELECT format('ALTER FUNCTION public.%I(%s) OWNER TO %I', proname,pg_get_function_identity_arguments(p.oid),:'migrator')
FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public'
  AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e') \gexec
COMMIT;

\connect :lake_db
SELECT pg_get_userbyid(datdba)=:'catalog' AS owner_matches FROM pg_database WHERE datname=current_database() \gset
\if :owner_matches
\else
    \if :adopt
    \else
        \echo 'Existing lake owner differs: review ownership transfer with DUCKFLEET_ADOPT_DATABASE_OWNER=yes'
        DO $refused$ BEGIN RAISE EXCEPTION 'database provisioning refused; see preceding diagnostic'; END $refused$;
    \endif
\endif
BEGIN;
ALTER DATABASE :"lake_db" OWNER TO :"catalog";
REVOKE ALL ON DATABASE :"lake_db" FROM PUBLIC,:"migrator",:"gateway",:"worker",:"maintainer";
REVOKE ALL ON SCHEMA public FROM PUBLIC;
ALTER SCHEMA public OWNER TO :"catalog";
SELECT format('ALTER %s public.%I OWNER TO %I', CASE relkind WHEN 'S' THEN 'SEQUENCE' WHEN 'v' THEN 'VIEW'
    WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE 'TABLE' END, relname, :'catalog')
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='public' AND relkind IN ('r','p','S','v','m')
  AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.deptype='e') \gexec
SELECT format('ALTER FUNCTION public.%I(%s) OWNER TO %I', proname,pg_get_function_identity_arguments(p.oid),:'catalog')
FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public'
  AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e') \gexec
COMMIT;
