//enable text log
!spool SF_ENV_SETUP/ENV_SETUP_FILES/mapping_data/log.txt;

//enable variable substitution
!set variable_substitution=true;

//db file location
!define db_mapping_file = "SF_ENV_SETUP/ENV_SETUP_FILES/mapping_data/DBNAME.csv";

//schema file location
!define scma_mapping_file = "SF_ENV_SETUP/ENV_SETUP_FILES/mapping_data/SCHEMANAME.csv";

//rbac file location
!define rbac_mapping_file = "SF_ENV_SETUP/ENV_SETUP_FILES/mapping_data/RBAC.csv";

//warehouse file location

!define wh_mapping_file = "SF_ENV_SETUP/ENV_SETUP_FILES/mapping_data/WAREHOUSE.csv";

USE ROLE SYSADMIN;

USE WAREHOUSE AUTOMATION_FRAMEWORK_WH;

put 'file://&{db_mapping_file}' @AUTOMATION_FRAMEWORK_DB.AFW_SCMA.INT_STG_AFW_ENV_SETUP OVERWRITE = TRUE;

put 'file://&{scma_mapping_file}' @AUTOMATION_FRAMEWORK_DB.AFW_SCMA.INT_STG_AFW_ENV_SETUP OVERWRITE = TRUE;

put 'file://&{rbac_mapping_file}' @AUTOMATION_FRAMEWORK_DB.AFW_SCMA.INT_STG_AFW_ENV_SETUP OVERWRITE = TRUE;

put 'file://&{wh_mapping_file}' @AUTOMATION_FRAMEWORK_DB.AFW_SCMA.INT_STG_AFW_ENV_SETUP OVERWRITE = TRUE;

//call ingestion proc
call  AUTOMATION_FRAMEWORK_DB.AFW_SCMA.p_ingest_mapping('&{db_mapping_file}','&{scma_mapping_file}','&{rbac_mapping_file}','&{wh_mapping_file}');
