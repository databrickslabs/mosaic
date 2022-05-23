# Contributing to the R bindings for Mosaic

The R bindings are automatically generated from the Scala code.

In order to generate the R files, run

```shell
Rscript --vanilla build_r_package.R
```

This will generate all the necessary R files in `sparkrMosaic/R/`.

## Style guide

Please refer to the [tidyverse style guide](https://style.tidyverse.org/index.html).
