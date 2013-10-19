#!/bin/bash

home=`dirname $0`
$home/../../bin/selection.sh $home/input /tmp/job-output Equals 1,2,3
diff -s $home/expected_output /tmp/job-output
