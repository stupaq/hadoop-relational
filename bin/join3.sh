#!/bin/bash

# Common procedure
. "`dirname $0`/common.sh"

# Run operator
[[ "$#" -lt 8 ]] && { echo "Missing arguments..."; exit 1; }

# Arguments:
# left table for join
input1="$1"
# middle table for join
input2="$2"
# right table for join
input3="$3"
# final output directory (local)
output="$4"
# key columns indices of left table
key1="$5"
# key columns indices for left join, left columns are used as a key for right join
key2="$6"
# key columns indices of right table
key3="$7"
# concurrency level, # of reducers = level^2
level="$8"
shift 8

hadoop jar "${relational_jar}" \
    "${root_package}.join3.Join3" \
    "${input1}" \
    "${input2}" \
    "${input3}" \
    "${temp_output}" \
    "${key1}" \
    "${key2}" \
    "${key3}" \
    "${level}"

rm -f "${output}"
hadoop dfs -getmerge "${temp_output}" "${output}"
