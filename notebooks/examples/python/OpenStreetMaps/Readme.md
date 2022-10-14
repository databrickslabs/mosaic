# Open Street Maps processing with Delta Live Tables

This is an example of how to ingest and process Open Street Maps using Databricks Delta Live Tablas and Mosaic.

The aim is to ingest and process OSM data and form the medallion layers of our delta lake following the best practices. Then used these tables to identify the density of residential buildings, hospitals and train stations across Italy as an example of the new insights this capability unlocks.

![Conceptual diagram](https://raw.githubusercontent.com/databrickslabs/mosaic/main/notebooks/examples/python/OpenStreetMaps/Images/Readme_ConceptDiagram.png)

The pipeline will
* Download the OSM dataset for Italy
* Extract from it the buildings (with relative metadata)
* Index the buildings on an H3 grid
* Separate the buildings by type
* Display a building density by counting the records per grid index

## Data flow

The pipeline is divided in three notebooks
* `0_Download` Will download the OSM dataset and ingest it into three delta tables
* `1_Process` Will create the Delta Live Table transformations to process the OSM data and extract the building shapes and metadata, index and categorise
* `2_Explore` Will display the density of residential and train station buildings

![Full pipeline](https://raw.githubusercontent.com/databrickslabs/mosaic/main/notebooks/examples/python/OpenStreetMaps/Images/Readme_FullPipeline.png)

## Results

You should be able to visualise the final result in the 2_Explore notebook

### Residential building density
![Building density](https://raw.githubusercontent.com/databrickslabs/mosaic/main/notebooks/examples/python/OpenStreetMaps/Images/Readme_BuildingDensity.png)

## Train station building density
![Train station density](https://raw.githubusercontent.com/databrickslabs/mosaic/main/notebooks/examples/python/OpenStreetMaps/Images/Readme_TrainStationDensity.png)
