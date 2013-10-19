#!/bin/bash

# Common procedure
. "`dirname $0`/common.sh"

# Run operator
[[ "$#" -lt 4 ]] && { echo "Missing arguments..."; exit 1; }

input="$1"
output="$2"
keys="$3"
aggregator="$4"
shift 4

hadoop jar "${relational_jar}" \
    "${root_package}.aggregation.Aggregation" \
    "${input}" \
    "${temp_output}" \
    "${keys}" \
    "${root_package}.aggregation.Aggregator\$${aggregator}" \
    "$@"

rm -f "${output}"
hadoop dfs -getmerge "${temp_output}" "${output}"
