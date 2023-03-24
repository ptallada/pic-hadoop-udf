package es.pic.hadoop.udf.adql;

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
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.google.common.geometry.S2LatLng;

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

    StructObjectInspector inputOI1;
    StructObjectInspector inputOI2;

    Converter ra1Converter;
    Converter dec1Converter;
    Converter ra2Converter;
    Converter dec2Converter;

    DoubleWritable ra1;
    DoubleWritable dec1;
    DoubleWritable ra2;
    DoubleWritable dec2;

    Object blob1;
    Object blob2;
    ADQLGeometry geom1;
    ADQLGeometry geom2;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 2) {
            if (!ObjectInspectorUtils.compareTypes(arguments[0], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(0, "First argument has to be of ADQL geometry type.");
            }
            inputOI1 = (StructObjectInspector) arguments[0];
            if (!ObjectInspectorUtils.compareTypes(arguments[1], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(1, "Second argument has to be of ADQL geometry type.");
            }
            inputOI2 = (StructObjectInspector) arguments[1];
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
            blob1 = arguments[0].get();
            blob2 = arguments[1].get();

            if (blob1 == null || blob2 == null) {
                return null;
            }

            geom1 = ADQLGeometry.fromBlob(blob1, inputOI1);
            geom2 = ADQLGeometry.fromBlob(blob2, inputOI2);

            if (!(geom1 instanceof ADQLPoint)) {
                throw new UDFArgumentTypeException(0,
                        String.format("First geometry is not a POINT, but a %s.", geom1.getKind().name()));
            }
            if (!(geom2 instanceof ADQLPoint)) {
                throw new UDFArgumentTypeException(1,
                        String.format("Second geometry is not a POINT, but a %s.", geom2.getKind().name()));
            }

            ra1 = geom1.getCoord(0);
            dec1 = geom1.getCoord(1);
            ra2 = geom2.getCoord(0);
            dec2 = geom2.getCoord(1);
        } else {
            ra1 = (DoubleWritable) ra1Converter.convert(arguments[0].get());
            dec1 = (DoubleWritable) dec1Converter.convert(arguments[1].get());
            ra2 = (DoubleWritable) ra2Converter.convert(arguments[2].get());
            dec2 = (DoubleWritable) dec2Converter.convert(arguments[3].get());
        }

        if (ra1 == null || dec1 == null || ra2 == null || dec2 == null) {
            return null;
        }

        return new DoubleWritable(S2LatLng.fromDegrees(dec1.get(), ra1.get())
                .getDistance(S2LatLng.fromDegrees(dec2.get(), ra2.get())).degrees());
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("distance", children);
    }
}
