//setup environment
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS AUTOMATION_FRAMEWORK_DB;

CREATE SCHEMA IF NOT EXISTS AUTOMATION_FRAMEWORK_DB.AFW_SCMA;

USE DATABASE AUTOMATION_FRAMEWORK_DB;

USE SCHEMA AFW_SCMA;

CREATE WAREHOUSE IF NOT EXISTS AUTOMATION_FRAMEWORK_WH WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 600 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD';

grant usage on warehouse "AUTOMATION_FRAMEWORK_WH" to role SECURITYADMIN;

grant usage on warehouse "AUTOMATION_FRAMEWORK_WH" to role useradmin;

USE WAREHOUSE AUTOMATION_FRAMEWORK_WH;

create STAGE AUTOMATION_FRAMEWORK_DB.AFW_SCMA.INT_STG_AFW_ENV_SETUP FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1,FIELD_OPTIONALLY_ENCLOSED_BY = '"');

create transient table if not exists AUTOMATION_FRAMEWORK_DB.AFW_SCMA.DATABASE_MAPPING_TTB (
DB_NAME varchar(200)
);

create transient table if not exists AUTOMATION_FRAMEWORK_DB.AFW_SCMA.SCHEMA_MAPPING_TTB (
DB_NAME varchar(200),
SCHEMA_NAME varchar(200)
);

create transient table if not exists AUTOMATION_FRAMEWORK_DB.AFW_SCMA.RBAC_MAPPING_TTB (
DB_NAME varchar(200),
SCHEMA_NAME varchar(200),
PRIVILEGE varchar(200),
FUNCTIONAL_ROLE varchar(200)
);

create transient table if not exists AUTOMATION_FRAMEWORK_DB.AFW_SCMA.WAREHOUSE_MAPPING_TTB (
FUNCTIONAL_ROLE varchar(200),
WAREHOUSE_SIZE varchar(20),
PRIVILEGE varchar(20),
MAX_CLUSTER varchar(20),
AUTO_SUSPEND varchar(20),
WH_SIZE_SUFFIX varchar(20)
);


create transient table if not exists "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA"."AFW_LOG_DETAILS_TTB"(ENVIRONMENT VARCHAR(20),EXECUTION_ID VARCHAR(60),TASK_ID VARCHAR(60),OBJECT_NAME VARCHAR(200),OBJECT_TYPE VARCHAR(30),STATUS VARCHAR(30),LOAD_TIME TIMESTAMP,REMARKS VARCHAR(200));

create transient table if not exists AUTOMATION_FRAMEWORK_DB.AFW_SCMA.AFW_LOG_SUMMARY_TTB(ENVIRONMENT VARCHAR(20),EXECUTION_ID VARCHAR(100),OBJECT_TYPE VARCHAR(50),LOAD_TIME TIMESTAMP,SUMMARY VARCHAR(250));


//procedure to ingest mapping table
create or replace procedure AUTOMATION_FRAMEWORK_DB.AFW_SCMA.p_ingest_mapping(DATABASE_MAPPING_TBL varchar(100),SCHEMA_MAPPING_TBL varchar(100), RBAC_MAPPING_TBL varchar(100), WAREHOUSE_MAPPING_TBL varchar(100))
  returns string
  language javascript
  execute as caller
  as
  $$
    var final_result = "Success"
       
    try
    {
        snowflake.execute ({sqlText: `USE DATABASE AUTOMATION_FRAMEWORK_DB`});

        snowflake.execute ({sqlText: `USE AUTOMATION_FRAMEWORK_DB.AFW_SCMA`});

        var database_name = DATABASE_MAPPING_TBL.split('/').pop();
        var schema_name = SCHEMA_MAPPING_TBL.split('/').pop();
        var rbac_name = RBAC_MAPPING_TBL.split('/').pop();
        var warehouse_name = WAREHOUSE_MAPPING_TBL.split('/').pop();

        snowflake.execute({sqlText: `truncate table AUTOMATION_FRAMEWORK_DB.AFW_SCMA.DATABASE_MAPPING_TTB`});

        snowflake.execute({sqlText: `truncate table AUTOMATION_FRAMEWORK_DB.AFW_SCMA.SCHEMA_MAPPING_TTB`});

        snowflake.execute({sqlText: `truncate table AUTOMATION_FRAMEWORK_DB.AFW_SCMA.RBAC_MAPPING_TTB`});

        snowflake.execute({sqlText: `truncate table AUTOMATION_FRAMEWORK_DB.AFW_SCMA.WAREHOUSE_MAPPING_TTB`});

        var my_sql_command = "copy into AUTOMATION_FRAMEWORK_DB.AFW_SCMA.DATABASE_MAPPING_TTB from @AUTOMATION_FRAMEWORK_DB.AFW_SCMA.INT_STG_AFW_ENV_SETUP/" + database_name;
        var statement = snowflake.createStatement( {sqlText: my_sql_command} );
        var result_set = statement.execute();
        
        my_sql_command = "copy into AUTOMATION_FRAMEWORK_DB.AFW_SCMA.SCHEMA_MAPPING_TTB from @AUTOMATION_FRAMEWORK_DB.AFW_SCMA.INT_STG_AFW_ENV_SETUP/" + schema_name;
        statement = snowflake.createStatement( {sqlText: my_sql_command} );
        result_set = statement.execute();
        
        my_sql_command = "copy into AUTOMATION_FRAMEWORK_DB.AFW_SCMA.RBAC_MAPPING_TTB from @AUTOMATION_FRAMEWORK_DB.AFW_SCMA.INT_STG_AFW_ENV_SETUP/" + rbac_name;
        statement = snowflake.createStatement( {sqlText: my_sql_command} );
        result_set = statement.execute();
        
        my_sql_command = "copy into AUTOMATION_FRAMEWORK_DB.AFW_SCMA.WAREHOUSE_MAPPING_TTB from @AUTOMATION_FRAMEWORK_DB.AFW_SCMA.INT_STG_AFW_ENV_SETUP/" + warehouse_name;
        statement = snowflake.createStatement( {sqlText: my_sql_command} );
        result_set = statement.execute();
    
    }
    catch(err)
    {
        final_result = "failed" + err.toString()

    }
    return final_result;
  $$
  ;

//env roles setup

  CREATE OR REPLACE PROCEDURE "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA".P_ENV_ROLE_SETUP(ENV STRING,JOB_ID STRING)
returns string
language javascript
EXECUTE AS CALLER
as 
$$
try
{
     var env=ENV.toLocaleUpperCase();

     var env_value = ENV.toLocaleUpperCase();

     var job_id=JOB_ID;

     if(env == 'PRD' || env == 'DEV')
     {
        env = "";
     }
     else
     {
        env += "_";
     }

     var role_dba = "GLOBAL_" + env +"ROLE_DBA";

     var admin_dba = "GLOBAL_" + env +"ADMIN_DBA";

     snowflake.execute ({sqlText: `USE ROLE SECURITYADMIN;`});

     snowflake.execute ({sqlText: `CREATE ROLE IF NOT EXISTS ${role_dba};`});

     snowflake.execute ({sqlText: `GRANT ROLE ${role_dba} TO ROLE SECURITYADMIN;`});

     snowflake.execute ({sqlText: `GRANT CREATE ROLE, CREATE USER ON ACCOUNT TO ROLE ${role_dba};`});

     snowflake.execute ({sqlText: `CREATE ROLE IF NOT EXISTS ${admin_dba};`});

     snowflake.execute ({sqlText: `GRANT ROLE ${admin_dba} TO ROLE SYSADMIN;`});

     snowflake.execute ({sqlText: `USE ROLE SYSADMIN;`});

     snowflake.execute ({sqlText: `GRANT CREATE DATABASE,CREATE WAREHOUSE ON ACCOUNT TO ROLE ${admin_dba};`});

     snowflake.execute ({sqlText: `grant usage on warehouse "AUTOMATION_FRAMEWORK_WH" to role ${role_dba};`});

     snowflake.execute ({sqlText: `grant usage on warehouse "AUTOMATION_FRAMEWORK_WH" to role ${admin_dba};`});

     snowflake.execute ({sqlText: `grant usage on database "AUTOMATION_FRAMEWORK_DB" to role ${admin_dba};`});

     snowflake.execute ({sqlText: `grant usage on database "AUTOMATION_FRAMEWORK_DB" to role ${role_dba};`});

     snowflake.execute ({sqlText: `grant usage on schema "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA" to role ${admin_dba};`});

     snowflake.execute ({sqlText: `grant usage on schema "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA" to role ${role_dba};`});

     snowflake.execute ({sqlText: `GRANT ALL ON ALL TABLES IN SCHEMA "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA" TO ROLE ${admin_dba};`});

     snowflake.execute ({sqlText: `GRANT ALL ON ALL TABLES IN SCHEMA "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA" TO ROLE ${role_dba};`});

     snowflake.execute ({sqlText: `GRANT ALL ON ALL PROCEDURES IN SCHEMA "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA" TO ROLE ${admin_dba};`});

     snowflake.execute ({sqlText: `GRANT ALL ON ALL PROCEDURES IN SCHEMA "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA" TO ROLE ${role_dba};`});

     var res =snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env_value}','${role_dba} and ${admin_dba}','Environment Roles','Success','Role Created','${job_id}');`});
 

}
catch(err)
{
    error_res = "ERROR : " + err.toString().replace(/'/g, '');

    snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env_value}','${role_dba} and ${admin_dba}','Environment Roles','Failure','${error_res}','${job_id}');`});

}
$$;




CREATE OR REPLACE PROCEDURE "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA".P_ENV_ROLE_GET(ENV STRING, JOB_ID STRING)
returns array not null
language javascript
EXECUTE AS CALLER
as 
$$
    var arr = new Array();
    try
    {
         var env=ENV.toLocaleUpperCase();

         var env_value = ENV.toLocaleUpperCase();

         var job_id=JOB_ID;

         if(env == 'PRD' || env == 'DEV')
         {
            env = "";
         }
         else
         {
            env += "_";
         }

         var role_dba = "GLOBAL_" + env +"ROLE_DBA";

         var admin_dba = "GLOBAL_" + env +"ADMIN_DBA";

         arr.push(role_dba);

         arr.push(admin_dba);
    }
    catch(err)
    {
        snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env_value}','ENV ROLES','Environment Roles','Failure','${error_res}','XX');`});

    }
    return arr;

$$
;

// database setup
CREATE OR REPLACE PROCEDURE "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA".P_DATABASE_SETUP(ENV STRING,JOB_ID STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS CALLER
AS
$$
   var env=ENV.toLocaleUpperCase();
   var job_id=JOB_ID;
   var db_name="";
   var acc_env="";
   var result=""; 
   var pass=0;
   var fail=0;
   var total=0;
   var exists=0;
 


   try{
        //code to set ADMIN_DBA role

        var env_roles = new Array();

        var env=ENV.toLocaleUpperCase();
        
        var env_roles_fetch = snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_ENV_ROLE_GET('${env}','${job_id}');`});
 
        env_roles_fetch.next();
        
        env_roles = env_roles_fetch.getColumnValue(1);
 
        var role_dba = env_roles[0];

        var admin_dba = env_roles[1];
         
         snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`});

         snowflake.execute ({sqlText: `USE DATABASE AUTOMATION_FRAMEWORK_DB`});

         snowflake.execute ({sqlText: `USE AUTOMATION_FRAMEWORK_DB.AFW_SCMA`});

         //code to fetch records from DATABASE_MAPPING_TTB table
         var my_sql_command = "select COALESCE(DB_NAME,'') AS DB_NAME from AUTOMATION_FRAMEWORK_DB.AFW_SCMA.DATABASE_MAPPING_TTB WHERE TRIM(COALESCE(DB_NAME,'')) <> ''";
         var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
         var result_set1 = statement1.execute();
         
         //Loop to iterate over the rows of Mapping Table with database names, create Database with appropriate  names
         while (result_set1.next())  
            {  
               //store value of Database names in variable
               var column1 = result_set1.getColumnValue(1);

               //store value in UpperCase
               var upper_column1 = column1.toLocaleUpperCase();

               //assign final Database names based on the rules if environment is Prod or not
               if (env == "PRD" || env == "DEV")
               db_name ="GLOBAL_"+upper_column1+"_DB";
               else
               db_name = "GLOBAL_"+env+"_"+upper_column1+"_DB";

              //code to create the database 
               try{    
                    total=total+1;
                    snowflake.execute ({sqlText: `CREATE DATABASE IF NOT EXISTS ${db_name}`});

                    //code to store the execution result to be passed in the procedure for AFW_LOG_DETAILS_TTB table
                    var my_sql_command2 = "select * from table(result_scan(last_query_id())) ";
                    var statement2 = snowflake.createStatement( {sqlText: my_sql_command2} );
                    var result_set2 = statement2.execute();
                    result_set2.next();
                    remark = result_set2.getColumnValue(1);
                    if(remark.includes("already exists"))
                    exists=exists+1;
                    else
                    pass=pass+1;
                    result = "Success";

                  } 
              //catch block for capturing the result status value in case of failure, to be passed in P_AFW_LOG_DETAILS table
               catch(e1)
                  {
                    var e2 = e1.toString();
                    var desired = e2.replace(/'/g, '');
                    remark = "Database creation Fail: "+desired;
                    result = "Fail";
                    fail=fail+1;
             
                   }
               
               //to call the procedure for inserting detailed execution results in AFW_LOG_DETAILS table
               try
               {
                    snowflake.execute({sqlText: "CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('"+env+"','"+db_name+"','Database','"+result+"','"+remark+"','"+job_id+"');"});
               }
               catch(err1){
                    return "Log call fail : " +err1;
               }

            }
          
              //to combine the final summary stats and call the procedure for inserting execution results in AFW_LOG_SUMMARY table
               try{
                   var last = total+" databases objects passed in mapping, "+pass+" created, "+exists+" already exists and "+fail+" failed";
                   snowflake.execute({sqlText:  "CALL  AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_SUMMARY('"+env+"','"+job_id+"','Database','"+last+"');"});
    
                  }
               catch(err)
                  {
                   return "Summary error : "+err;
                  }

            }
             catch(e2){
            return "Database Creation Failed with Error: " + e2 + e2.stackTraceTxt;
             }

          


   
    return "Setup created for env: "+env+" Successfully ";

 
$$;

//schema setup
create or replace procedure "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA".P_SCHEMA_SETUP(ENV STRING,JOB_ID STRING)
returns string not null
language javascript
EXECUTE AS CALLER
as     
$$  
  var env=ENV.toLocaleUpperCase();
  var job_id=JOB_ID;
  var db_name="";
  var sc_name="";
  var curr_acc = snowflake.createStatement( { sqlText: `SELECT CURRENT_ACCOUNT()` } ).execute();
  curr_acc.next();
  var curr_acc_name = curr_acc.getColumnValue(1);
  var remarks ="";
  var result=""; 
  var remark1="";
  var total = 0;
  var pass=0;
  var fail=0;
  var exists=0;
try
  {
     //code to set ADMIN_DBA role
     
     var env_roles = new Array();

     var env=ENV.toLocaleUpperCase();
        
     var env_roles_fetch = snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_ENV_ROLE_GET('${env}','${job_id}');`});
 
     env_roles_fetch.next();
        
     env_roles = env_roles_fetch.getColumnValue(1);
 
     var role_dba = env_roles[0];

     var admin_dba = env_roles[1];
         
     snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`});

     snowflake.execute ({sqlText: `USE DATABASE AUTOMATION_FRAMEWORK_DB`});

     snowflake.execute ({sqlText: `USE AUTOMATION_FRAMEWORK_DB.AFW_SCMA`});
     
     //code to fetch Schema names to be created and DB names from SCHEMA_MAPPING_TTB table
     var my_sql_command = "select COALESCE(DB_NAME,'') AS DB_NAME,COALESCE(SCHEMA_NAME,'') AS SCHEMA_NAME from AUTOMATION_FRAMEWORK_DB.AFW_SCMA.SCHEMA_MAPPING_TTB WHERE TRIM(COALESCE(DB_NAME,'')) <> '' AND TRIM(COALESCE(SCHEMA_NAME,'')) <> '';";
     var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
     var result_set1 = statement1.execute();
     
     //Loop to iterate over the rows of Mapping Table with schema and database names, create schemas with appropriate schema names 
     while (result_set1.next())  
     {  
        //code to store Database and schema names in variables
        var column1 = result_set1.getColumnValue(1);
        var column2 = result_set1.getColumnValue(2);

        //code to convert names to upper case   
        var upper_column1 = column1.toLocaleUpperCase();
        var upper_column2 = column2.toLocaleUpperCase();

        //assign final schema name based on the rule if the Database is LOAD Database or not 
        if (upper_column1 == "LOAD")
          sc_name = "GLOBAL_"+upper_column1+"_"+upper_column2+"_SCMA";
        else 
          sc_name = "GLOBAL_"+upper_column2+"_SCMA";
        
        //Database name based on the rule if the environment is Prod or not
        if (env == "PRD" || env == "DEV")
        {
             db_name ="GLOBAL_"+upper_column1+"_DB";
        }                 
        else
        {
            db_name = "GLOBAL_"+env+"_"+upper_column1+"_DB";
        }

        //variable to store fully qualified name of schema
        var sc = db_name+"."+sc_name;

        //code to create schema
        try
           { 
             total=total+1;
             snowflake.execute({sqlText: `CREATE SCHEMA IF NOT EXISTS ${db_name}.${sc_name} WITH MANAGED ACCESS;`});

             //code to store the result status of create schema command, to be passed in P_AFW_LOG_DETAILS table
             var my_sql_command2 = "select * from table(result_scan(last_query_id())) ";
             var statement2 = snowflake.createStatement( {sqlText: my_sql_command2} );
             var result_set2 = statement2.execute();
             result_set2.next();
             var remark1 = result_set2.getColumnValue(1); 
             if(remark1.includes("already exists"))
             exists=exists+1;
             else
             pass=pass+1;
             result = "Success";
           }
        //catch block for capturing the result status value in case of failure, to be passed in P_AFW_LOG_DETAILS table
        catch(e1)
           {
        
             var e2 = e1.toString();
             var desired = e2.replace(/'/g, '');
             //replacing ' with empty string
             remark1 = "Create Schema operation Fail "+desired ;
             fail=fail+1;
             result = "Failed" ;
            }
        
          //to call the procedure for inserting detailed execution results in AFW_LOG_DETAILS table
          try
            {
              snowflake.execute({sqlText: "CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('"+env+"','"+sc+"','Schema','"+result+"','"+remark1+"','"+job_id+"');"});
            }
          catch(err1)
            {
               return "Log call fail : " +err1;
            }
      }
       //to combine the final summary stats and call the procedure for inserting execution results in AFW_LOG_SUMMARY table
       try{
            var last = total+" schema objects passed in mapping, "+pass+" created, "+exists+" already exists and "+fail+" failed";
            
            snowflake.execute({sqlText:  "CALL  AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_SUMMARY('"+env+"','"+job_id+"','Schema','"+last+"');"});
           
         }
       catch(err)
         {
              return "Summary error : "+err;
         }
  }
  catch(e2)
  {
       return "ADMIN_DBA role not granted " +e2;
  }
       return "Schema created successfully"; 
$$
;

create or replace procedure "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA".P_WAREHOUSE_SETUP(ENV STRING,JOB_ID STRING)
returns string not null
language javascript
EXECUTE AS CALLER
as     
$$ 
    var final_result = "Success";
    
    try
    {
        var env_roles = new Array();

        var env = ENV.toLocaleUpperCase();
        
        var env_roles_fetch = snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_ENV_ROLE_GET('${env}','${JOB_ID}');`});
 
        env_roles_fetch.next();
        
        env_roles = env_roles_fetch.getColumnValue(1);
 
        var role_dba = env_roles[0];

        var admin_dba = env_roles[1];
         
        snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`});        

        snowflake.execute ({sqlText: `USE DATABASE AUTOMATION_FRAMEWORK_DB`});

        snowflake.execute ({sqlText: `USE AUTOMATION_FRAMEWORK_DB.AFW_SCMA`});
        
        var result_rows = snowflake.execute ({sqlText: `select COALESCE(FUNCTIONAL_ROLE,'') AS FUNCTIONAL_ROLE, COALESCE(WAREHOUSE_SIZE,'') AS WAREHOUSE_SIZE,COALESCE(PRIVILEGE,'') AS PRIVILEGE,MAX_CLUSTER,AUTO_SUSPEND,WH_SIZE_SUFFIX from AUTOMATION_FRAMEWORK_DB.AFW_SCMA.WAREHOUSE_MAPPING_TTB WHERE TRIM(COALESCE(FUNCTIONAL_ROLE,'')) <> '' AND TRIM(COALESCE(WAREHOUSE_SIZE,'')) <> '' AND TRIM(COALESCE(PRIVILEGE,'')) <> '';`});

        var warehouse_size_values = new Set(['XSMALL','SMALL','MEDIUM','LARGE','XLARGE','XXLARGE','XXXLARGE','X4LARGE','X5LARGE']) ;

        var wh_env = "";
        
        if(env.toLocaleUpperCase() == 'PRD' || env.toLocaleUpperCase() == 'DEV')
        {
            wh_env = ""; 
        }
        else
        {
            wh_env = env.trim().toLocaleUpperCase() + "_"; 
        }

        var warehouse_created = 0;

        while(result_rows.next())
        {            
            try
            {
                var functional_role_name =  result_rows.getColumnValue(1);
                var wh_size = result_rows.getColumnValue(2);
                var privilege_suffix = result_rows.getColumnValue(3);
                var wh_max_cluster = result_rows.getColumnValue(4);
                var wh_auto_suspend = result_rows.getColumnValue(5);
                var wh_size_suffix = result_rows.getColumnValue(6);
                var wh_privilege = "";

                if(wh_max_cluster != null && wh_max_cluster != undefined && !isNaN(wh_max_cluster))
                {
                    wh_max_cluster = wh_max_cluster;                    
                }
                else
                {
                    wh_max_cluster = "5";
                }

                if(wh_auto_suspend != null && wh_auto_suspend != undefined && !isNaN(wh_auto_suspend))
                {
                    wh_auto_suspend = wh_auto_suspend;                    
                }
                else
                {
                    wh_auto_suspend = "300";
                }

                if(wh_size_suffix != null && wh_size_suffix != undefined)
                {
                    
                    if((wh_size_suffix.trim().toLocaleUpperCase() != 'Y' || wh_size_suffix.trim().toLocaleUpperCase() != 'YES'))
                    {
                        wh_size_suffix = wh_size.trim().toLocaleUpperCase() +"_" ;
                    }
                    else
                    {
                        wh_size_suffix = "";
                    }
                                        
                }
                else
                {
                    wh_size_suffix = "";
                }

                if(privilege_suffix.trim().toLocaleUpperCase() == "USE")
                {
                    wh_privilege = "USAGE";                  
                }
                else if(privilege_suffix.trim().toLocaleUpperCase() == "OP")
                {
                    wh_privilege = "OPERATE";
                }
                else if(privilege_suffix.trim().toLocaleUpperCase() == "FULL")
                {
                    wh_privilege = "ALL";
                }
                else
                {
                    //error details
                    continue;
                }

                if(!warehouse_size_values.has(wh_size.trim().toLocaleUpperCase()))
                {
                    continue;
                }
                else
                {
                    wh_size = wh_size.trim().toLocaleUpperCase();
                }
                
                snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`});
                //GLOBAL_PROD_DATA_SCINECE_WH

                var wh_name = "GLOBAL_"+wh_env+ functional_role_name.trim().split(' ').join('_')+"_"+wh_size_suffix+"WH";

                snowflake.execute ({sqlText: `CREATE WAREHOUSE IF NOT EXISTS ${wh_name} WITH WAREHOUSE_SIZE = '${wh_size}' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = ${wh_auto_suspend} AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = ${wh_max_cluster} SCALING_POLICY = 'ECONOMY';`});

                snowflake.execute ({sqlText: `USE warehouse AUTOMATION_FRAMEWORK_WH`});

                snowflake.execute ({sqlText: `USE ROLE sysadmin`});
                
                snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env}','${wh_name}','WAREHOUSE','Success','Wareshouse created : ${wh_name}','${JOB_ID}');`});
                
                warehouse_created = warehouse_created+1;

                //GLOBAL_ENV_DATA_ENGINEER_STD_WH_USE_AR
                
                snowflake.execute ({sqlText: `USE ROLE ${role_dba}`});
                
                var wh_access_role = "GLOBAL_" +wh_env + functional_role_name.trim().split(' ').join('_')+"_" +"WH" + "_" + privilege_suffix + "_" + "AR";

                snowflake.execute ({sqlText: `CREATE ROLE IF NOT EXISTS ${wh_access_role}`}); 
                
                snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env}','${wh_name}','WAREHOUSE','Success','Warehouse Role created : ${wh_access_role}','${JOB_ID}');`});
                
                snowflake.execute ({sqlText: `GRANT ROLE ${wh_access_role} TO ROLE ${admin_dba}`});
                //GRANT WAREHOUSE TO ACCESS ROLE

                snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`});

                snowflake.execute ({sqlText: `grant ${wh_privilege} on warehouse ${wh_name} to role ${wh_access_role};`});

                //GRANT ACCESS ROLE TO FUNCTIONAL ROLE
                 snowflake.execute ({sqlText: `USE ROLE ${role_dba}`});
                
                //GLOBAL_ENV_DATA_ENGINEER_FR

                var wh_functional_role = "GLOBAL_" +wh_env+ functional_role_name.trim().split(' ').join('_')+"_"+ "FR";

                snowflake.execute ({sqlText: `CREATE ROLE IF NOT EXISTS ${wh_functional_role}`});    
                
                snowflake.execute ({sqlText: `GRANT ROLE ${wh_access_role} to ROLE ${wh_functional_role}`});
                
                snowflake.execute ({sqlText: `USE ROLE sysadmin`});

                snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env}','${wh_name}','WAREHOUSE','Success','Warehouse Access Role ${wh_access_role} granted to ${wh_functional_role}','${JOB_ID}');`});

            }
            catch(err)
            {
                error_res = "ERROR : " + err.toString().replace(/'/g, '');

                snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env}','WAREHOUSE','Warehouse','Failure','${error_res}' ,'${JOB_ID}');`});

            }
            
        }

        var summary_log = "Number of warehouse created/existing : " + warehouse_created;
        snowflake.execute({sqlText: `call AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_SUMMARY('${env}','${JOB_ID}','WAREHOUSE','${summary_log}');`});

    }
    catch(err)
    {
        final_result += "ERROR : " + err.toString().replace(/'/g, '');
    }
    return final_result;
$$;

//master procedure
CREATE OR REPLACE PROCEDURE "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA".P_AFW_SETUP(ENV STRING)
returns string not null
language javascript
EXECUTE AS CALLER
as 
$$
  var env=ENV.toLocaleUpperCase();
  var job_id="";
  var acc_env="";
  var curr_acc = snowflake.createStatement( { sqlText: `SELECT CURRENT_ACCOUNT()` } ).execute();
  curr_acc.next();
  var curr_acc_name = curr_acc.getColumnValue(1);
  
    if (curr_acc_name == "BELRONEUROPE" && (env != "PRD" || env != "PREPROD")) 
    {
      return "Invalid Environment Name for production account";
      
    }
    else 
    {
     acc_env = env;
    }
   
   //to check if environment entered is valid among the 4 environment options
   if(acc_env!= null && acc_env.length > 0)
   {
   
      try{
      //code to set ADMIN_DBA role

      var my_sql_command = "select MD5_HEX(current_timestamp(2)) ";
      var statement = snowflake.createStatement( {sqlText: my_sql_command} );
      var result_set = statement.execute();
      result_set.next();
      job_id = result_set.getColumnValue(1);
 
      snowflake.execute({sqlText: `call AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_ENV_ROLE_SETUP('${acc_env}','${job_id}')`});

      var env_roles = new Array();

      var env=ENV.toLocaleUpperCase();
        
      var env_roles_fetch = snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_ENV_ROLE_GET('${acc_env}','${job_id}');`});
 
      env_roles_fetch.next();
        
      env_roles = env_roles_fetch.getColumnValue(1);
 
      var role_dba = env_roles[0];

      var admin_dba = env_roles[1];
         
      snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`});

      snowflake.execute ({sqlText: `USE DATABASE AUTOMATION_FRAMEWORK_DB`});

      snowflake.execute ({sqlText: `USE AUTOMATION_FRAMEWORK_DB.AFW_SCMA`});
      
      //code to generate MD5 value to be used as the Execution ID for the current run 

   
        //code to call the procedure for Databases set up 
        try
        {
           
           snowflake.execute({sqlText: "CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_DATABASE_SETUP('"+env+"','"+job_id+"');"});
        }
        catch(e1)
        {
            return "Database Procedure Error: " +e1;
        }
        
        //code to call the procedure for Schema set up
        try
        {
           snowflake.execute({sqlText: "CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_SCHEMA_SETUP('"+env+"','"+job_id+"');"});
        }
        catch(e2)
        {
           return "Schema Procedure Error: "+e2;
        }
        
        try
        {
           
           snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_RBAC_SETUP('${env}');`});
        }
        catch(e4)
        {
           return "RBAC Procedure Error: "+e4;
        }

        try
        {           
           snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_WAREHOUSE_SETUP('${env}','${job_id}');`});
        }
        catch(e5)
        {
           return "Warehouse Procedure Error: "+e5;
        }
        
           }
          catch(e3){
            return "Sysadmin permission denied "+e3;
                   }
    }
     else
     {
       return "Invalid environment input";
     }
    
    
    
    return "Automation framework run complete, check AFW_LOG_DETAILS_TTB and AFW_LOG_SUMMARY_TTB for detailed logs "+env;
  $$;
  
//log details
CREATE OR REPLACE PROCEDURE "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA".P_AFW_LOG_DETAILS(ENV STRING ,OBJ_NAME STRING,OBJ_TYPE STRING,STATUS_VAL STRING,REMARKS STRING,JOB_ID STRING)
returns string not null
  language javascript
  EXECUTE AS CALLER
  as 
  $$
  var result = "Log Success"
  var env = ENV.toLocaleUpperCase();
  try
  {
      var obj_name = OBJ_NAME.toLocaleUpperCase();
      var obj_type = OBJ_TYPE;
      var  status = STATUS_VAL;
      var  remark = REMARKS;
      var job_id = JOB_ID;
      var my_sql_command2 = "INSERT INTO AUTOMATION_FRAMEWORK_DB.AFW_SCMA.AFW_LOG_DETAILS_TTB (ENVIRONMENT,EXECUTION_ID,TASK_ID,OBJECT_NAME,OBJECT_TYPE,STATUS,LOAD_TIME,REMARKS) SELECT $1,$2,MD5_HEX($3),$4,$5,$6,$7,$8  FROM VALUES('"+env+"','"+job_id+"',current_timestamp(2),'"+obj_name+"','"+obj_type+"','"+status+"',current_timestamp(2),'"+remark+"')";
      var statement2 = snowflake.createStatement( {sqlText: my_sql_command2} );
      var result_set2 = statement2.execute();
  }
  catch(err)
  {
      result = "Log Failure";
  }

  return result;   
 $$
  ;


CREATE OR REPLACE PROCEDURE "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA".P_AFW_LOG_SUMMARY(ENV STRING ,JOB_ID STRING,OBJ_TYPE STRING,SUMMARY STRING)
returns string not null
language javascript
EXECUTE AS CALLER
as 
$$
    var env = ENV.toLocaleUpperCase();
    var job_id = JOB_ID;
    var obj_type = OBJ_TYPE;  
    var summary = SUMMARY;
  
    try
       {
        //code to insert the record in AFW_LOG_SUMMARY_TTB table



        var my_sql_command2 = "INSERT INTO AUTOMATION_FRAMEWORK_DB.AFW_SCMA.AFW_LOG_SUMMARY_TTB VALUES('"+env+"','"+job_id+"','"+obj_type+"',current_timestamp,'"+summary+"')";
        var statement2 = snowflake.createStatement( {sqlText: my_sql_command2} );
        var result_set2 = statement2.execute();
       }
       
      //catch block for exception handling

    catch(err)
       {
        return "Summary error : "+err;
       }

    return "Summary Log Success";
   
$$
;

//FR setup
create or replace procedure AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_FR_SETUP(ENVIRONMENT_VALUE varchar(100),JOB_ID_VALUE STRING)
    returns string not null
    language javascript
    EXECUTE AS CALLER
    as
    $$
    var final_result = "Success";
    var no_of_created_roles = 0;
    var no_of_skipped_roles = 0;
    var no_of_skipped_records = 0;
    var job_id = '';
    try
    {
        var env_value = ENVIRONMENT_VALUE.toLocaleUpperCase();

        var env_append_value = ENVIRONMENT_VALUE.toLocaleUpperCase();

        var env_roles = new Array();
  
        var env_roles_fetch = snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_ENV_ROLE_GET('${env_value}','${job_id}');`});
 
        env_roles_fetch.next();
        
        env_roles = env_roles_fetch.getColumnValue(1);
 
        var role_dba = env_roles[0];

        var admin_dba = env_roles[1];
         
        snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`});
        
        job_id = JOB_ID_VALUE;

        if(env_value == 'PRD' || env_value == 'DEV')
         {
            env_append_value = "";
         }
         else
         {
            env_append_value += "_";
         } 

        //fetch data from RBAC_MAPPING_TBL for row
        var rbac_map = snowflake.execute({sqlText: `SELECT COALESCE(DB_NAME,'') AS DB_NAME,COALESCE(SCHEMA_NAME,'') AS SCHEMA_NAME,COALESCE(PRIVILEGE,'') AS PRIVILEGE,COALESCE(FUNCTIONAL_ROLE,'') AS FUNCTIONAL_ROLE FROM "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA"."RBAC_MAPPING_TTB" WHERE TRIM(COALESCE(DB_NAME,'')) <> '' AND TRIM(COALESCE(SCHEMA_NAME,'')) <> '' AND TRIM(COALESCE(PRIVILEGE,'')) <> '' AND TRIM(COALESCE(FUNCTIONAL_ROLE,'')) <> '' ;`});
        while(rbac_map.next())
        {
            if(rbac_map.getColumnValue(1) != null && rbac_map.getColumnValue(1).trim().length > 0 &&
                rbac_map.getColumnValue(2) != null && rbac_map.getColumnValue(2).trim().length >0 &&
                rbac_map.getColumnValue(3) != null && rbac_map.getColumnValue(3).trim().length >0 &&
                rbac_map.getColumnValue(4)!= null && rbac_map.getColumnValue(4).trim().length >0)
            {
                access_role_to_be_granted = "";
                //create AR structure
                if(rbac_map.getColumnValue(1)!= '' && rbac_map.getColumnValue(1).toLocaleUpperCase() == 'LOAD')
                {
                    access_role_to_be_granted = "GLOBAL"+ "_"+ env_append_value.trim() + rbac_map.getColumnValue(1).toLocaleUpperCase().trim() + "_" + rbac_map.getColumnValue(2).toLocaleUpperCase().trim() + "_" + rbac_map.getColumnValue(3).toLocaleUpperCase().trim()+ "_" + "AR";
                }
                else
                {
                    access_role_to_be_granted = "GLOBAL"+ "_"+ env_append_value.trim() + rbac_map.getColumnValue(2).toLocaleUpperCase().trim() + "_" + rbac_map.getColumnValue(3).toLocaleUpperCase().trim()+ "_" + "AR";
                }
                //split by comma
                var fr_row_data = rbac_map.getColumnValue(4).toLocaleUpperCase().split(',');

                var error_res = '';

                for(i = 0;i< fr_row_data.length;i++)
                {
                    try
                    {
                        //create FR format
                        snowflake.execute ({sqlText: `USE ROLE ${role_dba}`});

                        var functional_role_name =  "GLOBAL"+ "_"+ env_append_value.trim() + fr_row_data[i].trim().split(' ').join('_') + "_" + "FR";
                        
                        snowflake.execute({sqlText: `create role if not exists ${functional_role_name}`});

                        snowflake.execute({sqlText: `grant role ${functional_role_name} to role ${admin_dba}`});
                        //create role log
                        var res =snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env_value}','${functional_role_name}','Roles','Success','Role Created/Exists','${job_id}');`});
                        
                        no_of_created_roles+=1;
                        
                        if(access_role_to_be_granted!=null && access_role_to_be_granted.length > 0)
                        {
                            
                            snowflake.execute({sqlText: `grant role ${access_role_to_be_granted} to role ${functional_role_name}`});
                            //create grant role log
                            
                            snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env_value}','${functional_role_name}','Roles','Success','Granted Role : ${access_role_to_be_granted}','${job_id}');`});
                        }
                    }
                    catch(err)
                    {
                        
                        error_res = "ERROR : " + err.toString().replace(/'/g, '');
                        //error for skipped role
                        snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env_value}','${functional_role_name}','Roles','Failure','${error_res}' ,'${job_id}');`});
                    }
                }
            }
            else{
            no_of_skipped_records+=1;
            }
        }

        var summary_log = "Number of roles created/existing : " + no_of_created_roles + ". Number of records skipped : " + no_of_skipped_records;
        snowflake.execute({sqlText: `call AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_SUMMARY('${env_value}','${job_id}','Roles','${summary_log}');`});
    }
    catch(err)
    {
        final_result = "ERROR : " + err.toString().replace(/'/g, '');
        snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env_value}','RBAC : OUTER','Roles','Failure','${final_result}' ,'${job_id}');`});
    }
    return final_result;
    $$;
    

//rbac wrapper proc setup
create or replace procedure AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_RBAC_SETUP(ENVIRONMENT_VALUE varchar(100))
    returns string
    language javascript
    EXECUTE AS CALLER
    as
    $$
        var final_result = "Success";
        var job_id = '';
        var env_value = ENVIRONMENT_VALUE;
        try
        {   

            var env_value = ENVIRONMENT_VALUE.toLocaleUpperCase();

            var env_roles = new Array();

            var env_roles_fetch = snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_ENV_ROLE_GET('${env_value}','${job_id}');`});
 
            env_roles_fetch.next();
        
            env_roles = env_roles_fetch.getColumnValue(1);
    
            var role_dba = env_roles[0];

            var admin_dba = env_roles[1];
         
            snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`});

            snowflake.execute ({sqlText: `USE DATABASE AUTOMATION_FRAMEWORK_DB`});

            snowflake.execute ({sqlText: `USE AUTOMATION_FRAMEWORK_DB.AFW_SCMA`});
    
            job_id_result = snowflake.execute({sqlText: `select MD5_HEX(current_timestamp(2))`});            
            job_id_result.next();            
            job_id =  job_id_result.getColumnValue(1);

            var p_ar_result = snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AR_SETUP('${env_value}','${job_id}');`});
            p_ar_result.next();
            p_ar_result = p_ar_result.getColumnValue(1);            
            snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env_value}','P_AR_SETUP','RBAC PROCEDURE','${p_ar_result}','AR Procedure called','${job_id}');`});

            var p_fr_result = snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_FR_SETUP('${env_value}','${job_id}');`});
            p_fr_result.next();
            p_fr_result = p_fr_result.getColumnValue(1);
            snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env_value}','P_FR_SETUP','RBAC PROCEDURE','${p_fr_result}','FR Procedure called','${job_id}');`});

        }
        catch(err)
        {
            final_result = "ERROR : " + err.toString().replace(/'/g, '');
            snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('${env_value}','RBAC : MASTER','','Failure','${final_result}' ,'${job_id}');`});
        }

        return final_result;
    $$;

//AR setup
CREATE OR REPLACE PROCEDURE "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA".P_AR_SETUP(ENV VARCHAR, JOB_ID VARCHAR) 
returns string not null
language javascript
execute as CALLER
as
$$
// ------------------------------------------------------------
// variables 
// ------------------------------------------------------------
try{
    var ctr_created = 0;

    var ctr_exists = 0;

    var env_roles = new Array();

    var env=ENV.toLocaleUpperCase();
        
    var env_roles_fetch = snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_ENV_ROLE_GET('${env}','${JOB_ID}');`});
 
    env_roles_fetch.next();
        
    env_roles = env_roles_fetch.getColumnValue(1);
 
    var role_dba = env_roles[0];

    var admin_dba = env_roles[1];
         
    snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`});

    snowflake.execute ({sqlText: `USE DATABASE AUTOMATION_FRAMEWORK_DB`});

    snowflake.execute ({sqlText: `USE AUTOMATION_FRAMEWORK_DB.AFW_SCMA`});

    var db_schma = snowflake.execute ({sqlText: `select UPPER(COALESCE(DB_NAME,'')) AS DTBS,UPPER(COALESCE(SCHEMA_NAME,'')) AS SCHMA from AUTOMATION_FRAMEWORK_DB.AFW_SCMA.SCHEMA_MAPPING_TTB WHERE TRIM(COALESCE(DB_NAME,'')) <> '' AND TRIM(COALESCE(SCHEMA_NAME,'')) <> '';`});
    if(db_schma.getRowCount() > 0){
        
        while(db_schma.next()){
            
            try {
                var IN_DB = db_schma.getColumnValue(1);
                var IN_SCMA = db_schma.getColumnValue(2);
                var DB = '';
                var SCMA = '';
                var RO_AR = '';
                var RW_AR = '';
                var FULL_AR = '';
                if(IN_DB == 'LOAD'){
                    if(ENV == 'PRD' || ENV == 'DEV'){
                        DB = 'GLOBAL_LOAD_DB';
                        SCMA = 'GLOBAL_LOAD_' + IN_SCMA + '_SCMA';
                        RO_AR   = 'GLOBAL' + '_LOAD_' + IN_SCMA + '_RO_AR';
                        RW_AR   = 'GLOBAL' + '_LOAD_' + IN_SCMA + '_RW_AR';
                        FULL_AR = 'GLOBAL' + '_LOAD_' + IN_SCMA + '_FULL_AR';
                        } 
                    else{
                        DB = 'GLOBAL_' + ENV +'_LOAD_DB';
                        SCMA = 'GLOBAL_LOAD_' + IN_SCMA + '_SCMA';
                        RO_AR   = 'GLOBAL_' + ENV + '_LOAD_' + IN_SCMA + '_RO_AR';
                        RW_AR   = 'GLOBAL_' + ENV + '_LOAD_' + IN_SCMA + '_RW_AR';
                        FULL_AR = 'GLOBAL_' + ENV + '_LOAD_' + IN_SCMA + '_FULL_AR';
                        }
                }
                else if(IN_DB == 'REPORTING'){
                    if(ENV == 'PRD' || ENV == 'DEV'){
                        DB = 'GLOBAL_REPORTING_DB';
                        SCMA = 'GLOBAL_' + IN_SCMA + '_SCMA';
                        RO_AR   = 'GLOBAL_' + IN_SCMA + '_RO_AR';
                        RW_AR   = 'GLOBAL_' + IN_SCMA + '_RW_AR';
                        FULL_AR = 'GLOBAL_' + IN_SCMA + '_FULL_AR';
                        } 
                    else{
                        DB = 'GLOBAL_' + ENV +'_REPORTING_DB';
                        SCMA = 'GLOBAL_' + IN_SCMA + '_SCMA';                        
                        RO_AR   = 'GLOBAL_' + ENV + '_' + IN_SCMA + '_RO_AR';
                        RW_AR   = 'GLOBAL_' + ENV + '_' + IN_SCMA + '_RW_AR';
                        FULL_AR = 'GLOBAL_' + ENV + '_' + IN_SCMA + '_FULL_AR';
                        }
                }
                snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`});
                
                var checkdb = snowflake.execute ({sqlText: `show databases LIKE '` + DB + `'`});
                if(checkdb.next()){
                    var checkscma = snowflake.execute ({sqlText: `SHOW SCHEMAS LIKE '` + SCMA + `' IN DATABASE ` + DB});
                    if(checkscma.next()){
                        
                        try{
                            snowflake.execute ({sqlText: `USE ROLE ${role_dba}`});
                        
                            var RO_CREATE = snowflake.execute ({sqlText: `create role if not exists ` + RO_AR});
                            RO_CREATE.next();
                            
                            if(!RO_CREATE.getColumnValue(1).includes('already exists')){
                                
                                  
                                snowflake.execute ({sqlText: `grant role ` + RO_AR + `  to role ${admin_dba}`});
                                snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`});   
                                snowflake.execute ({sqlText: `grant usage on database ` + DB + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant usage on schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant select on all tables in schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant select on all views  in schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant usage, read on all stages in  schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant usage on all file formats in  schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant select on all streams in  schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant usage on all functions in schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant select on future tables in  schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant select on future views  in  schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant usage, read on future stages in schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant usage on future file formats in schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant select on future streams in schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant usage on future procedures in schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                snowflake.execute ({sqlText: `grant usage on future functions in  schema ` + DB + `.` + SCMA + ` to role `+ RO_AR});
                                
                                ctr_created=ctr_created+1;
                                
                                snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('`+ENV+`','`+RO_AR+`','Access Role','Success','Access role  created','`+JOB_ID+`')`});
                            }
                        
                                
                            else {
                                ctr_exists = ctr_exists +1 ;
                                snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('`+ENV+`','`+RO_AR+`','Access Role','Success','Access role already exists','`+JOB_ID+`')`});
                            }
                            
                        }
                        catch(err_ar_ro){
                            
                            snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('`+ENV+`','`+RO_AR+`','Access Role','Fail','Error while creating and setting privileges to Access role','`+JOB_ID+`')`});
                        }
                        
                        try{
                            snowflake.execute ({sqlText: `USE ROLE ${role_dba}`});
                            
                            var RW_CREATE = snowflake.execute ({sqlText: `create role if not exists ` + RW_AR});
                            RW_CREATE.next();   
                            if(!RW_CREATE.getColumnValue(1).includes('already exists')){                            
                                    
                                snowflake.execute ({sqlText: `grant role `+ RW_AR + `  to role ${admin_dba}`});
                                snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`}); 
                                snowflake.execute ({sqlText: `grant usage on database ` + DB + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant usage on schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant select, insert, update, delete, references on all tables in schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant select on all views  in schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant usage, read, write on all stages in schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant usage on all file formats in  schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant select on all streams in  schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant usage on all procedures in  schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant usage on all functions in schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant usage on all sequences in schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant monitor, operate on all tasks in  schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant select, insert, update, delete, references on future tables in schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant select on future views  in  schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant usage, read, write on future stages in  schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant usage on future file formats in schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant select on future streams in schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant usage on future procedures in schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant usage on future functions in  schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant usage on future sequences in  schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                snowflake.execute ({sqlText: `grant monitor, operate on future tasks in schema ` + DB + `.` + SCMA + ` to role `+ RW_AR});
                                
                                ctr_created=ctr_created+1;
                                
                                snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('`+ENV+`','`+RW_AR+`','Access Role','Success','Access role  created','`+JOB_ID+`')`});
                            }
                            
                            else {
                                ctr_exists = ctr_exists +1 ;
                                snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('`+ENV+`','`+RW_AR+`','Access Role','Success','Access role already exists','`+JOB_ID+`')`});
                            }
                        }
                        catch(err_ar_rw){
                            snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('`+ENV+`','`+RW_AR+`','Access Role','Fail','Error while creating and setting privileges to Access role','`+JOB_ID+`')`});
                        }
                        
                        
                        try{
                        
                            snowflake.execute ({sqlText: `USE ROLE ${role_dba}`});
                            
                            var FULL_CREATE = snowflake.execute ({sqlText: `create role if not exists ` + FULL_AR});
                            FULL_CREATE.next(); 
                            
                            if(!FULL_CREATE.getColumnValue(1).includes('already exists')){                                
                                
                                snowflake.execute ({sqlText: `grant role `+ FULL_AR + `  to role ${admin_dba}`});
                                snowflake.execute ({sqlText: `USE ROLE ${admin_dba}`});
                                snowflake.execute ({sqlText: `grant usage on database ` + DB + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all privileges  on schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on all tables in  schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on all views  in  schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on all stages in  schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on all file formats in  schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on all streams in schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on all procedures in  schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on all functions in schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on all sequences in schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on all tasks in schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on future tables in schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on future views  in schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on future stages in schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on future file formats in schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on future streams in  schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on future procedures in schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on future functions in  schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on future sequences in  schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                snowflake.execute ({sqlText: `grant all on future tasks in      schema ` + DB + `.` + SCMA + ` to role `+ FULL_AR});
                                ctr_created=ctr_created+1;
                                
                                snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('`+ENV+`','`+FULL_AR+`','Access Role','Success','Access role  created','`+JOB_ID+`')`});
                            
                            }
                            
                            else {
                                ctr_exists = ctr_exists +1 ;
                                snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('`+ENV+`','`+FULL_AR+`','Access Role','Success','Access role already exists','`+JOB_ID+`')`});
                            }
                            
                        }
                        catch(err_ar_full){
                            snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('`+ENV+`','`+FULL_AR+`','Access Role','Fail','Error while creating and setting privileges to Access role','`+JOB_ID+`')`});
                        }
                        
                    }
                    else{
                        snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('`+ENV+`','`+DB+`.`+SCMA+`','Access Role','Fail','Schema not exist, Could not create roles','`+JOB_ID+`')`});
                    }
                    
                }
                else{
                    snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('`+ENV+`','`+DB+`.`+SCMA+`','Access Role','Fail','Database not exist, Could not create roles','`+JOB_ID+`')`});
                }
                
                    
            }
            
            catch(err_ar1){
                snowflake.execute({sqlText: `CALL AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_DETAILS('`+ENV+`','`+SCMA+`','Access Role','Fail','Error while executing iteration for schema','`+JOB_ID+`')`});
            }
            
        }
    snowflake.execute({sqlText:  `CALL  AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_SUMMARY('`+ENV+`','`+JOB_ID+`','Access Role','` + ctr_created + ` Roles created, `+ ctr_exists +` Roles already exists')`});
    }
    
    
    else {
        snowflake.execute({sqlText:  `CALL  AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_SUMMARY('`+ENV+`','`+JOB_ID+`','Access Role','No Valid Input Found from Mapping Table');`});
    }
    
    
    return "SUCCESS";
}
catch(err_ar){
    snowflake.execute({sqlText:  `CALL  AUTOMATION_FRAMEWORK_DB.AFW_SCMA.P_AFW_LOG_SUMMARY('`+ENV+`','`+JOB_ID+`','Access Role','Stored Procedure ran into error');`});
    return "FAIL";
}
$$;

grant usage on database "AUTOMATION_FRAMEWORK_DB" to role SECURITYADMIN;
grant usage on database "AUTOMATION_FRAMEWORK_DB" to role useradmin;
grant usage on schema "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA" to role SECURITYADMIN;
grant usage on schema "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA" to role useradmin;
GRANT ALL ON ALL TABLES IN SCHEMA "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA" TO ROLE SECURITYADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA" TO ROLE useradmin;
GRANT ALL ON ALL PROCEDURES IN SCHEMA "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA" TO ROLE SECURITYADMIN;
GRANT ALL ON ALL PROCEDURES IN SCHEMA "AUTOMATION_FRAMEWORK_DB"."AFW_SCMA" TO ROLE useradmin;



