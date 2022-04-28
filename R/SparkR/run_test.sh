#!/bin/bash
Rscript --vanilla sparkrMosaic/tests/not_devtools_tests.R
if (($? != 0))
then
	exit 1
fi

