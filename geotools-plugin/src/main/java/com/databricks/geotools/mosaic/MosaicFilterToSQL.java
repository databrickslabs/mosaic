package com.databricks.geotools.mosaic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.geotools.filter.FilterCapabilities;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.jdbc.PreparedFilterToSQL;
import org.geotools.jdbc.PreparedStatementSQLDialect;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Coordinates;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.BinarySpatialOperator;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;

import com.databricks.geotools.DatabricksFilterToSQL;
import com.databricks.geotools.DatabricksSQLDialect;
import com.databricks.geotools.LayersList.TableInfo;
//import com.uber.h3core.H3Core;
//import com.uber.h3core.util.GeoCoord;


/**
 * MosaicFilterToSQL for Databricks Lakehouse.
 *
 * @author Tom Diepenbrock <tom.diepenbrock@databricks.com>
 */
public class MosaicFilterToSQL extends DatabricksFilterToSQL {

	private Logger LOGGER;
//	private H3Core h3;
	
	public MosaicFilterToSQL(PreparedStatementSQLDialect dialect) {
		super(dialect);
//		try {
//			h3 = H3Core.newInstance();
//		}
//		catch(IOException ex) {
//			throw new RuntimeException(ex);
//		}
		
		this.LOGGER = org.geotools.util.logging.Logging.getLogger(getClass());
		LOGGER.fine("in MosaicFilterToSQL(PreparedStatementSQLDialect dialect)!");
		// TODO Auto-generated constructor stub
	}

	
	@Override
    protected FilterCapabilities createFilterCapabilities() {
        // Databricks does not actually implement all of the special functions
		LOGGER.fine("in createFilterCapabilities()!");

        FilterCapabilities caps = super.createFilterCapabilities();
        caps.addType(BBOX.class);
        caps.addType(Contains.class);
//        caps.addType(Crosses.class);
//        caps.addType(Disjoint.class);
//        caps.addType(Equals.class);
        caps.addType(Intersects.class);
//        caps.addType(Overlaps.class);
//        caps.addType(Touches.class);
//        caps.addType(Within.class);
//        caps.addType(Beyond.class);

        return caps;
    }
	
	@Override
    protected void visitLiteralGeometry(Literal expression) throws IOException {
		LOGGER.fine("in visitLiteralGeometry()!");
        Geometry g = (Geometry) evaluateLiteral(expression, Geometry.class);
        LOGGER.fine("geometry class: " + g.getClass().getName() + "; geometry string: " + g.toText());
        if (g instanceof LinearRing) {
            // WKT does not support linear rings
            g = g.getFactory().createLineString(((LinearRing) g).getCoordinateSequence());
        }
        out.write("st_geomfromwkt('" + g.toText() + "', " + currentSRID + ")");
    }

    /**
     * Handles the more general case of two generic expressions.
     *
     * <p>The most common case is two PropertyName expressions, which happens during a spatial join.
     */
    protected Object visitBinarySpatialOperator(
            BinarySpatialOperator filter, Expression e1, Expression e2, boolean swapped, Object extraData) {
    	LOGGER.fine("in visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, Expression e2, boolean swapped, Object extraData)!");
    	
    	try {
    		LOGGER.fine("e1:");
    		LOGGER.fine(e1.toString());
    		LOGGER.fine(e1.getClass().getName());
    		
    		LOGGER.fine("e2:");
    		LOGGER.fine(e2.toString());
    		LOGGER.fine(e2.getClass().getName());
    		
    		LOGGER.fine("swapped: " + swapped);
    		
    		LOGGER.fine("Table Name: " + getFeatureType().getTypeName());
    		LOGGER.fine("Schema Name:" + getDatabaseSchema());
    		
    		TableInfo tblInfo = ((DatabricksSQLDialect)this.dialect).getLayersList().getTableInfo(getDatabaseSchema(), getFeatureType().getTypeName());
    		boolean useIdx = true;
    		if (tblInfo.getMosaicIndexColumn() == null || tblInfo.getMosaicIndexResolution() == 0) {
    			useIdx = false;
    		}
    		
    		LOGGER.fine(((org.geotools.filter.LiteralExpressionImpl)e2).getValue().getClass().getName());
    		
    		if (filter instanceof BBOX) {
    			if (useIdx) 
                	out.write("st_intersects_mosaic(");
                else
                	out.write("st_intersects(");
    		}
    		else if (!(filter instanceof BBOX)) {
                if (filter instanceof Contains) {
                    if (useIdx) 
                    	out.write("st_contains_mosaic(");
                    else
                    	out.write("st_contains(");
                } else if (filter instanceof Crosses) {
                    out.write("st_crosses(");
                } else if (filter instanceof Disjoint) {
                    out.write("st_disjoint(");
                } else if (filter instanceof Equals) {
                    out.write("st_equals(");
                } else if (filter instanceof Intersects) {
                    out.write("st_intersects_mosaic(");
                } else if (filter instanceof Overlaps) {
                    out.write("st_overlaps(");
                } else if (filter instanceof Touches) {
                    out.write("st_touches(");
                } else if (filter instanceof Within) {
                    out.write("st_within(");
                } else {
                    throw new RuntimeException("Unknown operator: " + filter);
                }
    		}
    		
    		if (
    				(filter instanceof BBOX || filter instanceof Contains || filter instanceof Intersects) && useIdx
    				//the below is commented out because the FilterToSQL superclass actually does this check
    				//in order to determine whether to enter this function or not, but it helps make the following
    				//code clearer to see it here:
    				//
    				//&& e1 instanceof Literal && e2 instanceof PropertyName  
    				) {
    			
    			//In both cases below, we need to use the mosaic index column instead of the geometry column
            	//and short circuit the accept/etc paradigm for that column
    			if (swapped) {
    				//This means that someone wants to do st_[contains | intersects]_mosaic([geometry literal], [geometry column].
        			//GeoTools will swap the order of the two arguments to visitBinarySpatialOperator in this case,
        			//and e2 will be our geometry literal and e1 is the geometry column, so we need to build our
        			//st_[contains | intersects]_mosaic expression accordingly.
    				out.write("mosaicfill(");
    				e2.accept(this, extraData); //this will return "st_geomfromwkt([WKT])"
    				out.write(", " + tblInfo.getMosaicIndexResolution() + ")");
    				out.write(", ");
    				//e1.accept(this, extraData); //don't call this, use the mosaic index column instead
    				out.write(tblInfo.getMosaicIndexColumn());
    				out.write(")");
	            } else {
	            	//here e1 will be the geometry column. Substitute the mosaic index column instead and don't call
	            	//accept() on the geometry column
	                //e1.accept(this, extraData);
	            	out.write(tblInfo.getMosaicIndexColumn());
	                out.write(",  mosaicfill(");
	                e2.accept(this, extraData);
	                out.write(", " + tblInfo.getMosaicIndexResolution() + "))");
	            }
    		}
    		else {
	    		if (swapped) {
	                e2.accept(this, extraData);
	                out.write(", ");
	                e1.accept(this, extraData);
	            } else {
	                e1.accept(this, extraData);
	                out.write(", ");
	                e2.accept(this, extraData);
	            }
	            out.write(")");
    		}
    	}
    	catch (IOException e) {
            throw new RuntimeException(e);
        }
    return extraData;
    }
    
}
