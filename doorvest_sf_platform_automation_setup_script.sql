//enable text log
!spool mapping_data/log.txt;

//enable variable substitution
!set variable_substitution=true;


//sql environment setup file location
!define env_setup_file = "doorvest_sf_env_setup.sql";

//setup environment by calling file
!source &{env_setup_file};



