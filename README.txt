1) For this automation, a new database BELRON_AUTOMATION_FRAMEWORK_DB and new schema AFW_SCMA will be created.
2) The schema AFW_SCMA contains 9 stored procedures and 5 tables, created as a part of automation setup.
3) Default path where the utility should be unzipped is C:/.
4) The utility is triggered through snowsql which is a prerequisite.
5) After connection to snowflake is done, following steps need to be applied to setup automation framwork on account.

	execute command : !source {filepath}/belron/belron_sf_platform_automation_setup_script;
	
6) After connection to snowflake is done, following steps need to be applied to setup environment on account. Environment input 
is expected from user that needs to be filled in the last line of the script belron_sf_environment_setup_script.sql. For example, 
call BELRON_AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_SETUP('PRD') --> to create environment setup for Production;
Lastly execute below command.

	!source {filepath}/belron/belron_sf_environment_setup_script.sql;

	

7) Input mapping for database,schema,access roles, functional roles can be provided through files in /belron/mapping_data
	: please note that files names and their formats along with the headers should not be changed.
