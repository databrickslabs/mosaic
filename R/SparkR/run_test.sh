#!/bin/bash
Rscript --vanilla sparkrMosaic/tests/test_spark.R
if (($? != 0))
then
	exit 1
fi

