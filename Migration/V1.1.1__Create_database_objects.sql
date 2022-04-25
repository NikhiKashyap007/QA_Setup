create or replace database github_demo_db;
use database github_demo_db;
use role accountadmin;
grant all on database github_demo_db to role sysadmin;
use role sysadmin;
create schema temp_Schema;
