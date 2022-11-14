package es.pic.hadoop.udf.adql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.geometry.S1Angle;
import com.google.common.geometry.S2Cap;
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
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

// @formatter:off
@Description(
    name = "area",
    value = "_FUNC_(geom) -> area:double",
    extended = "Compute the area, in square degrees, of a given geometry."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFArea extends GenericUDF {
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    final static StandardUnionObjectInspector geomOI = ADQLGeometry.OI;

    double ra;
    double dec;

    Object geom;
    ADQLGeometry.Kind kind;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 1) {
            if (arguments[0] != geomOI) {
                throw new UDFArgumentTypeException(0, "Argument has to be of ADQL geometry type.");
            }
        } else {
            throw new UDFArgumentLengthException("This function takes a single argument: geometry");
        }

        return doubleOI;
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
            return new DoubleWritable(0);

        case CIRCLE:
            @SuppressWarnings("unchecked")
            List<DoubleWritable> circle_coords = (List<DoubleWritable>) geomOI.getField(geom);

            double ra = circle_coords.get(0).get();
            double dec = circle_coords.get(1).get();
            double radius = circle_coords.get(2).get();

            S2Cap circle = S2Cap.fromAxisAngle(S2LatLng.fromDegrees(dec, ra).toPoint(), S1Angle.degrees(radius));

            return new DoubleWritable(circle.area() * (180 / Math.PI) * (180 / Math.PI));

        case POLYGON:
            @SuppressWarnings("unchecked")
            List<DoubleWritable> poly_coords = (List<DoubleWritable>) geomOI.getField(geom);
            List<S2Point> points = new ArrayList<S2Point>();

            for (int i = 0; i < poly_coords.size(); i += 2) {
                ra = poly_coords.get(i).get();
                dec = poly_coords.get(i + 1).get();
                points.add(S2LatLng.fromDegrees(dec, ra).toPoint());
            }

            S2Loop loop = new S2Loop(points);

            return new DoubleWritable(loop.getArea() * (180 / Math.PI) * (180 / Math.PI));

        case REGION:
        default:
            throw new UnsupportedOperationException("Operations on regions are not yet supported");
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("area", children);
    }
}
