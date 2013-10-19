#!/bin/bash

home=`dirname $0`
$home/../../bin/aggregation.sh $home/input /tmp/job-output 0,1 Sum
diff -s $home/expected_output /tmp/job-output
