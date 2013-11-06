#!/bin/bash

# Common procedure
. "`dirname $0`/common.sh"

# Run operator
[[ "$#" -lt 8 ]] && { echo "Missing arguments..."; exit 1; }

input1="$1"
input2="$2"
input3="$3"
output="$4"
key1="$5"
key2="$6"
key3="$7"
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
