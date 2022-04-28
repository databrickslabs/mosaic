#!/bin/bash
Rscript --vanilla sparkrMosaic/tests/tests.R
if (($? != 0))
then
	exit 1
fi

