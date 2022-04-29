//temp

!source SF_ENV_SETUP/ENV_SETUP_FILES/doorvest_sf_platform_automation_setup_script.sql

!source SF_ENV_SETUP/ENV_SETUP_FILES/doorvest_sf_environment_setup_script.sql
use role accountadmin;
call AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_SETUP('{{ VAR_ENV }}');
