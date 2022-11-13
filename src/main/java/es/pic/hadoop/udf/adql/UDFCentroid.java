package es.pic.hadoop.udf.adql;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.io.DoubleWritable;

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
    final static StandardUnionObjectInspector geomOI = ADQLGeometry.OI;

    Object geom;
    ADQLGeometry.Kind kind;
    Object centroid;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 1) {
            if (arguments[0] != geomOI) {
                throw new UDFArgumentTypeException(0, "Argument has to be of ADQL geometry type.");
            }
        } else {
            throw new UDFArgumentLengthException("This function takes a single argument: geometry");
        }

        return geomOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        geom = arguments[0].get();

        if (geom == null) {
            return null;
        }

        kind = ADQLGeometry.Kind.valueOfTag(geomOI.getTag(geom));

        switch (kind) {
        case POINT:
            return null;

        case CIRCLE:
            @SuppressWarnings("unchecked")
            List<DoubleWritable> circle_coords = (List<DoubleWritable>) geomOI.getField(geom);

            centroid = geomOI.create();
            geomOI.setFieldAndTag(centroid, circle_coords.subList(0, 2), ADQLGeometry.Kind.POINT.tag);

            return centroid;

        case POLYGON:
            @SuppressWarnings("unchecked")
            List<DoubleWritable> poly_coords = (List<DoubleWritable>) geomOI.getField(geom);
            List<S2Point> points = new ArrayList<S2Point>();

            double ra;
            double dec;

            for (int i = 0; i < poly_coords.size(); i += 2) {
                ra = poly_coords.get(i).get();
                dec = poly_coords.get(i + 1).get();
                points.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }

            S2Loop loop = new S2Loop(points);
            S2LatLng point = new S2LatLng(loop.getCentroid());

            List<DoubleWritable> coords = Arrays.asList(new DoubleWritable[] {
                    new DoubleWritable(point.lngDegrees()), new DoubleWritable(point.latDegrees())
            });

            centroid = geomOI.create();
            geomOI.setFieldAndTag(centroid, coords, ADQLGeometry.Kind.POINT.tag);

            return centroid;

        case REGION:
        default:
            throw new UnsupportedOperationException("Operations on regions are not yet supported");
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("centroid", children);
    }
}
