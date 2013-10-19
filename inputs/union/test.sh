#!/bin/bash

home=`dirname $0`
$home/../../bin/union.sh $home/input1 $home/input2 /tmp/job-output
diff -s $home/expected_output /tmp/job-output
