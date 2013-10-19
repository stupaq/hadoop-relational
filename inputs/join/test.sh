#!/bin/bash

home=`dirname $0`
$home/../../bin/join.sh $home/input1 $home/input2 /tmp/job-output 0 2
diff -s $home/expected_output /tmp/job-output
