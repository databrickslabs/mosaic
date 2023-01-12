package com.databricks.geotools;

import com.databricks.geotools.carto.CartoDialect;
import com.databricks.geotools.mosaic.MosaicDialect;
import com.databricks.geotools.sedona.SedonaDialect;

public enum DatabricksGISLibrary {
	SEDONA(SedonaDialect.class.getName()),
	CARTO(CartoDialect.class.getName()),
	MOSAIC(MosaicDialect.class.getName()),
	;
	
	public static final DatabricksGISLibrary[] VALUES = values();
	
	public final String value;

	DatabricksGISLibrary(String value) {
	    this.value = value;
	  }
	
}

