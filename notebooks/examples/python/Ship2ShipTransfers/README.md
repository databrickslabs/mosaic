# Ship2Ship Transfer Detection
![Ship Overlap](./images/kepler_output.png)
## Introduction
This is an algorithmic implementation to detect Ship to Ship transfers at scale using Databricks. It was presented at the [Data and AI Summit 2022](https://databricks.com/dataaisummit/session/mosaic-framework-geospatial-analytics-scale). 

This Mosaic example explores a novel, algorithmic approach to detecting Ship to Ship transfers at scale using AIS data. In particular it aims to surpass existing, naive implementations that are just based on a particular distance radius like the one shown below:

![Naive Approach](./images/naive_approach.png)

Although the naive approach can be optimised with indices to be quite performant, additional improvements can be made. This is quite apparent if we look at the following data points below:

![Naive Approach with buffers](./images/buffer_approach.png)

According to our naive approach, where we buffer around our LAT/LONG points, the two vessels would not intersect. However, if we construct the actual path the vessels took, our algorithmic implementation would detect an overlap between the two paths, as shown below:

![Path Line Strings approach](./images/linestring_approach.png)

This model is expanded upon in the course of the attached notebooks. It shows how to ingest AIS data, how to process it at scale leveraging Mosaic in Databricks, and provides examples of how to extend the analysis to incorporate additional sources. 

## Requirements
This notebook series was run on Databricks with the [Mosaic](https://github.com/databrickslabs/mosaic/tree/main/python) library installed. For this example it is recommended to install it from [PyPi](https://pypi.org/project/databricks-mosaic/) on your Databricks cluster like [so](https://docs.databricks.com/libraries/cluster-libraries.html).  

*created by [@tiems90](https://github.com/tiems90)* 