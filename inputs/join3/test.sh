#!/bin/bash

home=`dirname $0`
$home/../../bin/join3.sh $home/input1 $home/input2 $home/input3 /tmp/job-output 1 0 0 2
diff -s $home/expected_output /tmp/job-output
