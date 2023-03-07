package es.pic.hadoop.udf.adql;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

// @formatter:off
@Description(
    name = "polygon",
    value = "_FUNC_(ra1:double, dec1:double, ra2:double, dec2:double, ra3:double, dec3:double, ...) | _FUNC_(pt1:point, pt2:point, p3:point, ...) -> polygon:ADQLGeometry",
    extended = "Construct an ADQL polygon from a sequence of at least 3 sky coordinates."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFPolygon extends GenericUDF {
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

    List<Converter> coordConverters;
    List<DoubleWritable> coordArgs;

    boolean has_points = false;
    boolean has_coords = false;

    DoubleWritable coordArg;
    Object geom;
    ADQLGeometry.Kind kind;

    Object polygon;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length >= 3) {
            ObjectInspector oi;

            coordConverters = new ArrayList<Converter>();
            for (int i = 0; i < arguments.length; i++) {
                oi = arguments[i];

                if (ObjectInspectorUtils.compareTypes(oi, ADQLGeometry.OI)) {
                    has_points = true;
                } else {
                    has_coords = true;
                    coordConverters.add(ObjectInspectorConverters.getConverter(oi, doubleOI));
                }
            }

            if (has_coords && has_points) {
                throw new UDFArgumentTypeException(0, "All arguments must be either ADQLGeometry or double.");
            }

            if (has_coords && (arguments.length % 2 == 1)) {
                throw new UDFArgumentLengthException(
                        "If ra and dec values are passed, there must be an even number of arguments.");
            }

        } else {
            throw new UDFArgumentLengthException(
                    "This function takes at least 3 arguments: either (ra1, dec1, ra2, dec2, ra3, dec3, ...) or (pt1, pt2, pt3, ...).");
        }

        if (has_coords && arguments.length < 6) {
            throw new UDFArgumentTypeException(0,
                    "If less than 6 arguments are provided, they all must be ADQLGeometry.");
        }

        return ADQLGeometry.OI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        coordArgs = new ArrayList<DoubleWritable>();
        if (has_coords) {
            for (int i = 0; i < arguments.length; i++) {
                coordArg = (DoubleWritable) coordConverters.get(i).convert(arguments[i].get());

                if (coordArg == null) {
                    return null;
                }

                coordArgs.add(coordArg);
            }
        } else {
            for (int i = 0; i < arguments.length; i++) {
                geom = arguments[i].get();

                if (geom == null) {
                    return null;
                }

                kind = ADQLGeometry.getTag(geom);

                if (kind != ADQLGeometry.Kind.POINT) {
                    throw new UDFArgumentTypeException(i,
                            String.format("Provided geometry is not a POINT, but a %s.", kind.name()));
                }

                List<DoubleWritable> coords = ADQLGeometry.getCoords(geom);

                coordArgs.add(coords.get(0));
                coordArgs.add(coords.get(1));
            }
        }

        return new ADQLPolygon(coordArgs).serialize();
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("polygon", children);
    }
}
