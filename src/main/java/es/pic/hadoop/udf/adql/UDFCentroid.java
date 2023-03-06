package es.pic.hadoop.udf.adql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

// @formatter:off
@Description(
    name = "centroid",
    value = "_FUNC_(geom) -> centroid:ADQLGeometry",
    extended = "Compute the centroid of a given geometry."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFCentroid extends GenericUDF {
    Object geom;
    ADQLGeometry.Kind kind;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 1) {
            if (!ObjectInspectorUtils.compareTypes(arguments[0], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(0, "Argument has to be of ADQL geometry type.");
            }
        } else {
            throw new UDFArgumentLengthException("This function takes a single argument: geometry");
        }

        return ADQLGeometry.OI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        geom = arguments[0].get();

        if (geom == null) {
            return null;
        }

        kind = ADQLGeometry.getTag(geom);

        switch (kind) {
        case POINT:
            return null;

        case CIRCLE:
            List<DoubleWritable> circle_coords = ADQLGeometry.getCoords(geom);

            return new ADQLPoint(circle_coords.subList(0, 2)).serialize();

        case POLYGON:
            List<DoubleWritable> poly_coords = ADQLGeometry.getCoords(geom);
            List<S2Point> vertices = new ArrayList<S2Point>();

            double ra;
            double dec;

            for (int i = 0; i < poly_coords.size(); i += 2) {
                ra = poly_coords.get(i).get();
                dec = poly_coords.get(i + 1).get();
                vertices.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }

            S2Loop loop = new S2Loop(vertices);
            S2LatLng point = new S2LatLng(loop.getCentroid());

            return new ADQLPoint(point.lngDegrees(), point.latDegrees()).serialize();

        case REGION:
        default:
            throw new UnsupportedOperationException("REGIONs are not supported");
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("centroid", children);
    }
}
