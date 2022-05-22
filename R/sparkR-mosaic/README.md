# sparkrMosaic 

sparkrMosaic is a lightweight extension of SparkR to expose Mosaic's large scale geospatial data processing functions.

## Getting started

### Requirements
The only requirement to start using Mosaic is a Databricks cluster running Databricks Runtime 10.0 (or later) with the Mosaic Scala JAR attached and the sparkrMosaic package installed.

Both the R package and JAR can be found in the 'Releases' section of the Mosaic GitHub repository.

Instructions for how to attach JARs to a Databricks cluster can be found [here](https://docs.databricks.com/libraries/cluster-libraries.html).

Installing the R package requires a manual installation via `install.packages()`. This is most easily achieved by moving the R package into DBFS from Github. 

The following code block illustrates an installation;

```
# SparkR is preinstalled on Databricks
library(SparkR)

sparkr_mosaic_package_path = '/Users/<my-user-name>/sparkrMosaic.tar.gz'
install.packages(sparkr_mosaic_package_path, repos=NULL)
library(sparkrMosaic)
```

### Example notebooks
This repository contains several example notebooks in `notebooks/examples`. You can import them into your Databricks workspace using the instructions [here](https://docs.databricks.com/notebooks/notebooks-manage.html#import-a-notebook).

### Contributing to the R bindings for Mosaic

See the [contributing](./CONTRIBUTING.md) section. 