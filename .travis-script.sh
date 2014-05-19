#!/usr/bin/env bash
# This script is executed by our parent assembly, which is assumed to be bdevel

context=src/python/bshotgun/tests
# the first time it generates a database cache, the second time it uses it
for i in `seq 2`; do
	bin/posix/be @$context go nosetests || exit
done
