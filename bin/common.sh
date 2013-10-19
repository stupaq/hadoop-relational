#!/bin/sh

# Configuration
relational_jar="target/relational-0.1-SNAPSHOT.jar"
root_package="pl.stupaq.hadoop.relational"
temp_output="/tmp/relational_job_output-`date +%s`"

# Prepare inputs
hadoop dfs -rmr inputs/ 1>&2 2>/dev/null
hadoop dfs -put inputs/ inputs/ 1>&2 2>/dev/null

# Register cleanup
trap "hadoop dfs -rmr ${temp_output} 2>/dev/null" EXIT

# Fail early from now
set -e
