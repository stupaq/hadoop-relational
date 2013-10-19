 relational algebra implementation in Hadoop
=============================================

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

Usage
-----

Scripts under `bin/` are meant to serve as documentation.
