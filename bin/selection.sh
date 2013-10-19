#!/bin/bash

# Common procedure
. "`dirname $0`/common.sh"

# Run operator
[[ "$#" -lt 3 ]] && { echo "Missing arguments..."; exit 1; }

input="$1"
output="$2"
predicate="$3"
shift 3

hadoop jar "${relational_jar}" \
    "${root_package}.selection.Selection" \
    "${input}" \
    "${temp_output}" \
    "${root_package}.selection.Predicate\$${predicate}" \
    "$@"

rm -f "${output}"
hadoop dfs -getmerge "${temp_output}" "${output}"
