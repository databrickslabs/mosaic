package com.databricks.geotools;

import java.io.IOException;

import org.geotools.geometry.jts.JTS;
import org.geotools.jdbc.PreparedFilterToSQL;
import org.geotools.jdbc.PreparedStatementSQLDialect;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BinarySpatialOperator;

public abstract class DatabricksFilterToSQL extends PreparedFilterToSQL {
	
	public DatabricksFilterToSQL(PreparedStatementSQLDialect dialect) {
		super(dialect);
		this.LOGGER = org.geotools.util.logging.Logging.getLogger(getClass());
		LOGGER.fine("in DatabricksFilterToSQL(PreparedStatementSQLDialect dialect)!");
		// TODO Auto-generated constructor stub
	}
	
	/** Handles the common case of a PropertyName,Literal geometry binary spatial operator. */
	@Override
    protected Object visitBinarySpatialOperator(
            BinarySpatialOperator filter,
            PropertyName property,
            Literal geometry,
            boolean swapped,
            Object extraData) {
		LOGGER.fine("in visitBinarySpatialOperator(BinarySpatialOperator filter,\n"
				+ "            PropertyName property,\n"
				+ "            Literal geometry,\n"
				+ "            boolean swapped,\n"
				+ "            Object extraData)!");
        return visitBinarySpatialOperator(filter, property, (Expression)geometry, swapped, extraData);
    }

    @Override
    protected Object visitBinarySpatialOperator(
            BinarySpatialOperator filter, Expression e1, Expression e2, Object extraData) {
    	LOGGER.fine("in visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, Expression e2, Object extraData)!");
    	return visitBinarySpatialOperator(filter, e1, e1, false, extraData);
    }
    
    protected abstract Object visitBinarySpatialOperator(
            BinarySpatialOperator filter, Expression e1, Expression e2, boolean swapped, Object extraData);
    
    protected Class getTargetClassFromContext(Object context) {
        if (context instanceof Class) {
            return (Class) context;
        } else if (context instanceof AttributeDescriptor) {
            return ((AttributeDescriptor) context).getType().getBinding();
        }
        return null;
    }
    
    @Override
    public Object visit(Literal expression, Object context) throws RuntimeException {
    	LOGGER.fine("IN VISIT!!");
        if (!isPrepareEnabled()) return super.visit(expression, context);

        Class clazz = getTargetClassFromContext(context);

        // evaluate the literal and store it for later
        Object literalValue = evaluateLiteral(expression, clazz);

        // bbox filters have a right side expression that's a ReferencedEnvelope,
        // but SQL dialects use/want polygons instead
        if (literalValue instanceof Envelope && convertEnvelopeToPolygon()) {
            clazz = Polygon.class;
            literalValue = JTS.toGeometry((Envelope) literalValue);
        }

        if (clazz == null && literalValue != null) {
            clazz = literalValue.getClass();
        }

        literalValues.add(literalValue);
        SRIDs.add(currentSRID);
        dimensions.add(currentDimension);
        descriptors.add(
                context instanceof AttributeDescriptor ? (AttributeDescriptor) context : null);
        literalTypes.add(clazz);

        try {
            if (literalValue == null || dialect == null) {
                out.write("?");
            } else {
                StringBuffer sb = new StringBuffer();
                if (Geometry.class.isAssignableFrom(literalValue.getClass())) {
                    int srid = currentSRID != null ? currentSRID : -1;
                    int dimension = currentDimension != null ? currentDimension : -1;
                    dialect.prepareGeometryValue(
                            (Geometry) literalValue, dimension, srid, Geometry.class, sb);
                } else if (encodingFunction) {
                    dialect.prepareFunctionArgument(clazz, sb);
                } else {
                    sb.append("?");
                }
                out.write(sb.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return context;
    }
}
