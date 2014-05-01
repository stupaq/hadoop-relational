Relational algebra implementation in Hadoop
===========================================

Relation format
---------------

Relations are stored in flat, textual form. Each line maps to a tuple, elements
are separated with commas (CSV file without header).

Building and testing
--------------------

Build using `mvn package` (which will also run integration tests build on top
of JUnit) run `mvn package -DskipTests` if you wish to skip unit tests.

Scripts under `bin/` might be useful for running on a real cluster.

Each subdirectory of `inputs/` contains example inputs and expected outputs
together with nullary script `test.sh` which runs tests on default Hadoop
installation available to the user.

Tested on Arch Linux installation with AUR package `hadoop`.

Usage and side notes
--------------------

Scripts under `bin/` are meant to serve as documentation.
This is the right place to look for arguments explanation and other instructions.

One note about specifying columns: when you define a key for join, (sub)set of
columns for projection or aggregation, you should separate column indices
(starting from zero) with a comma ','; no trailing or leading comma allowed.

One tricky place is three-way-join since middle relation consists of two disjoint
set of attributes, one of them being a key for left join and the other for right
one. The convention here is that join indices for middle relation that you specify
denote left join indices. Rest of the attributes form right join key.

Copyright (c) 2014 Mateusz Machalica
