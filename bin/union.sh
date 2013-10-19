#!/bin/bash

# Common procedure
. "`dirname $0`/common.sh"

# Run operator
[[ "$#" -lt 3 ]] && { echo "Missing arguments..."; exit 1; }

input1="$1"
input2="$2"
output="$3"
shift 3

hadoop jar "${relational_jar}" \
    "${root_package}.union.Union" \
    "${input1}" \
    "${input2}" \
    "${temp_output}"

rm -f "${output}"
hadoop dfs -getmerge "${temp_output}" "${output}"
