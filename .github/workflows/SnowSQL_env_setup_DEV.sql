name: SnowSQL_env_setup_DEV
env:
  SNOWSQL_DEST: ~/snowflake
  SNOWSQL_ACCOUNT: ${{ secrets.DEV_SF_ACCOUNT }}
  SNOWSQL_USER: ${{ secrets.DEV_SF_USERNAME }}
  SNOWSQL_PWD: ${{ secrets.DEV_SF_PASSWORD }}
  
on:
  push:
    branches:
      - DEV
    paths:
      - 'SF_ENV_SETUP/SCRIPT_FOR_ENV_SETUP/'                                                  
jobs:                         
  executequery:                           
    name: Install SnowSQL                          
    runs-on: ubuntu-latest                           
    steps:
    - name: Checkout
      uses: actions/checkout@master
    - name: Download SnowSQL
      run:  curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.9-linux_x86_64.bash
    - name: Install SnowSQL
      run: SNOWSQL_DEST=~/snowflake SNOWSQL_LOGIN_SHELL=~/.profile bash snowsql-1.2.9-linux_x86_64.bash
    - name: Test installation
      run:  ~/snowflake/snowsql -v
    - name: Execute SQL against Snowflake
      run:  ~/snowflake/snowsql -f $GITHUB_WORKSPACE/SF_ENV_SETUP/SCRIPT_FOR_ENV_SETUP/ENV_setup_script.sql;
