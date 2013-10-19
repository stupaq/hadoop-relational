#!/bin/bash

# Common procedure
. "`dirname $0`/common.sh"

# Run operator
[[ "$#" -lt 5 ]] && { echo "Missing arguments..."; exit 1; }

input1="$1"
input2="$2"
output="$3"
key1="$4"
key2="$5"
shift 5

hadoop jar "${relational_jar}" \
    "${root_package}.join.Join" \
    "${input1}" \
    "${input2}" \
    "${temp_output}" \
    "${key1}" \
    "${key2}"

rm -f "${output}"
hadoop dfs -getmerge "${temp_output}" "${output}"
