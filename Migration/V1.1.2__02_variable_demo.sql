use database {{ DB_NAME }};
use schema {{ SCHEMA_NAME }};

CREATE OR REPLACE PROCEDURE temp_proc()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
snowflake.execute({sqlText: "use database {{ DB_NAME }}"});
snowflake.execute({sqlText: "use schema {{ SCHEMA_NAME }}"});
var query= "create table {{ DB_NAME }}.{{ SCHEMA_NAME }}.Temp_table (id int)";
snowflake.execute({sqlText: query});
$$
;
