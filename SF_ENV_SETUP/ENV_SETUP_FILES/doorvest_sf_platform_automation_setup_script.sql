//enable text log
!spool SF_ENV_SETUP/ENV_SETUP_FILES/mapping_data/log.txt;

//enable variable substitution
!set variable_substitution=true;


//sql environment setup file location
!define env_setup_file = "SF_ENV_SETUP/ENV_SETUP_FILES/doorvest_sf_env_setup.sql";

//setup environment by calling file
!source &{env_setup_file};



