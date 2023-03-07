package es.pic.hadoop.udf.adql;

import java.util.List;

import com.google.common.geometry.S2LatLng;

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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

// @formatter:off
@Description(
    name = "circle",
    value = "_FUNC_(ra1:double, dec1:double, ra2:double, dec2:double) | _FUNC_(pt1:point, pt2:point) -> distance:double",
    extended = "Compute the arc length along a great circle between two sky coordinates."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFDistance extends GenericUDF {
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

    Converter ra1Converter;
    Converter dec1Converter;
    Converter ra2Converter;
    Converter dec2Converter;

    DoubleWritable ra1Arg;
    DoubleWritable dec1Arg;
    DoubleWritable ra2Arg;
    DoubleWritable dec2Arg;

    Object geom1;
    Object geom2;
    ADQLGeometry.Kind kind1;
    ADQLGeometry.Kind kind2;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 2) {
            if (!ObjectInspectorUtils.compareTypes(arguments[0], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(0, "First argument has to be of ADQL geometry type.");
            }
            if (!ObjectInspectorUtils.compareTypes(arguments[1], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(1, "Second argument has to be of ADQL geometry type.");
            }
        } else if (arguments.length == 4) {
            ra1Converter = ObjectInspectorConverters.getConverter(arguments[0], doubleOI);
            dec1Converter = ObjectInspectorConverters.getConverter(arguments[1], doubleOI);
            ra2Converter = ObjectInspectorConverters.getConverter(arguments[2], doubleOI);
            dec2Converter = ObjectInspectorConverters.getConverter(arguments[3], doubleOI);
        } else {
            throw new UDFArgumentLengthException(
                    "This function takes 2 or 4 arguments: either (pt1, pt2) or (ra1, dec1, ra2, dec2)");
        }

        return doubleOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments.length == 2) {
            geom1 = arguments[0].get();
            geom2 = arguments[1].get();

            if (geom1 == null || geom2 == null) {
                return null;
            }

            kind1 = ADQLGeometry.getTag(geom1);
            kind2 = ADQLGeometry.getTag(geom2);

            if (kind1 != ADQLGeometry.Kind.POINT) {
                throw new UDFArgumentTypeException(0,
                        String.format("First geometry is not a POINT, but a %s.", kind1.name()));
            }
            if (kind2 != ADQLGeometry.Kind.POINT) {
                throw new UDFArgumentTypeException(1,
                        String.format("Second geometry is not a POINT, but a %s.", kind2.name()));
            }

            List<DoubleWritable> coords1 = ADQLGeometry.getCoords(geom1);
            List<DoubleWritable> coords2 = ADQLGeometry.getCoords(geom2);

            ra1Arg = coords1.get(0);
            dec1Arg = coords1.get(1);
            ra2Arg = coords2.get(0);
            dec2Arg = coords2.get(1);
        } else {
            ra1Arg = (DoubleWritable) ra1Converter.convert(arguments[0].get());
            dec1Arg = (DoubleWritable) dec1Converter.convert(arguments[1].get());
            ra2Arg = (DoubleWritable) ra2Converter.convert(arguments[2].get());
            dec2Arg = (DoubleWritable) dec2Converter.convert(arguments[3].get());
        }

        if (ra1Arg == null || dec1Arg == null || ra2Arg == null || dec2Arg == null) {
            return null;
        }

        return new DoubleWritable(S2LatLng.fromDegrees(dec1Arg.get(), ra1Arg.get())
                .getDistance(S2LatLng.fromDegrees(dec2Arg.get(), ra2Arg.get())).degrees());
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("distance", children);
    }
}
