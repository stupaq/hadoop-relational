#!/bin/bash

home=`dirname $0`
$home/../../bin/projection.sh $home/input /tmp/job-output 2,0
diff -s $home/expected_output /tmp/job-output
