package com.databricks.geotools.carto;

import java.io.IOException;
import java.util.logging.Logger;

import org.geotools.filter.FilterCapabilities;
import org.geotools.geometry.jts.JTS;
import org.geotools.jdbc.PreparedFilterToSQL;
import org.geotools.jdbc.PreparedStatementSQLDialect;
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


/**
 * CartoFilterToSQL for Databricks Lakehouse.
 *
 * @author Tom Diepenbrock <tom.diepenbrock@databricks.com>
 */
public class CartoFilterToSQL extends DatabricksFilterToSQL {

	private Logger LOGGER;
	
	public CartoFilterToSQL(PreparedStatementSQLDialect dialect) {
		super(dialect);
		this.LOGGER = org.geotools.util.logging.Logging.getLogger(getClass());
		LOGGER.fine("in CartoFilterToSQL(PreparedStatementSQLDialect dialect)!");
		// TODO Auto-generated constructor stub
	}

	
	@Override
    protected FilterCapabilities createFilterCapabilities() {
        // Databricks does not actually implement all of the special functions
		LOGGER.fine("in createFilterCapabilities()!");

        FilterCapabilities caps = super.createFilterCapabilities();
        caps.addType(BBOX.class);
        caps.addType(Contains.class);
        caps.addType(Crosses.class);
        caps.addType(Disjoint.class);
        caps.addType(Equals.class);
        caps.addType(Intersects.class);
        caps.addType(Overlaps.class);
        caps.addType(Touches.class);
        caps.addType(Within.class);
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
        out.write("st_geomFromWKT('" + g.toText() + "', " + currentSRID + ")");
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
    		if (filter instanceof BBOX) {
    			 out.write("st_intersects(");
    		}
    		else if (!(filter instanceof BBOX)) {
                if (filter instanceof Contains) {
                    out.write("st_contains(");
                } else if (filter instanceof Crosses) {
                    out.write("st_crosses(");
                } else if (filter instanceof Disjoint) {
                    out.write("st_disjoint(");
                } else if (filter instanceof Equals) {
                    out.write("st_equals(");
                } else if (filter instanceof Intersects) {
                    out.write("st_intersects(");
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
    	catch (IOException e) {
            throw new RuntimeException(e);
        }
    return extraData;
    }
     
}
