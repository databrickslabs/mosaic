# Open Street Maps processing with Delta Live Tables

This is an example of how to ingest and process Open Street Maps using Databricks Delta Live Tablas and Mosaic.

The objective of this example is processing the OSM dataset to identify the density of residential buildings, hospitals and train stations across Italy.

![Conceptual diagram](https://github.com/databrickslabs/mosaic/raw/feature/open_street_maps/notebooks/examples/python/OpenStreetMaps/Images/Readme_ConceptDiagram.png)

The pipeline will
* Download the OSM dataset for Italy
* Extract from it the buildings (with relative metadata)
* Index the buildings on an H3 grid
* Separate the buindings by type
* Display a building density by counting the records per grid index

## Data flow

The pipeline is divided in three notebooks
* 0_Download - Will download the OSM dataset and ingest it into three delta tables
* 1_Process - Will create the Delta Live Table transformations to process the OSM data and extract the building shapes and metadata, index and categorise
* 2_Explore - Will display the density of residential and train station buildings

![Full pipeline](https://github.com/databrickslabs/mosaic/raw/feature/open_street_maps/notebooks/examples/python/OpenStreetMaps/Images/Readme_FullPipeline.png)

## Results

You should be able to visualise the final result in the 2_Explore notebook

![Building density](https://github.com/databrickslabs/mosaic/raw/feature/open_street_maps/notebooks/examples/python/OpenStreetMaps/Images/Readme_BuildingDensity.png)

![Train station density](https://github.com/databrickslabs/mosaic/raw/feature/open_street_maps/notebooks/examples/python/OpenStreetMaps/Images/Readme_TrainStationDensity.png)
