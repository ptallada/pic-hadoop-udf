package es.pic.hadoop.udf.healpix;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

// @formatter:off
@Description(
    name = "vec2ang",
    value = "_FUNC_(x:float, y:float, z:float, [lonlat:bool=False]) -> array<double>(theta/ra, phi/dec)",
    extended = "Return the angular coordinates corresponding to this 3D position vector."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFVec2Ang extends GenericUDF {
    Converter xConverter;
    Converter yConverter;
    Converter zConverter;
    Converter lonlatConverter;

    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    final static ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

    DoubleWritable xArg;
    DoubleWritable yArg;
    DoubleWritable zArg;
    BooleanWritable lonlatArg = new BooleanWritable();

    double x;
    double y;
    double z;
    boolean lonlat;

    DoubleWritable theta = new DoubleWritable();
    DoubleWritable phi = new DoubleWritable();
    DoubleWritable ra = new DoubleWritable();
    DoubleWritable dec = new DoubleWritable();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 3 || arguments.length > 4) {
            throw new UDFArgumentLengthException(
                    "This function takes at least 3 arguments, no more than 4: x, y, z, lonlat");
        }

        xConverter = ObjectInspectorConverters.getConverter(arguments[0], doubleOI);
        yConverter = ObjectInspectorConverters.getConverter(arguments[1], doubleOI);
        zConverter = ObjectInspectorConverters.getConverter(arguments[2], doubleOI);

        if (arguments.length == 4) {
            lonlatConverter = ObjectInspectorConverters.getConverter(arguments[3], boolOI);
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        xArg = (DoubleWritable) xConverter.convert(arguments[0].get());
        yArg = (DoubleWritable) yConverter.convert(arguments[1].get());
        zArg = (DoubleWritable) zConverter.convert(arguments[2].get());

        if (arguments.length == 4) {
            lonlatArg = (BooleanWritable) lonlatConverter.convert(arguments[3].get());
        }
        if (xArg == null || yArg == null || zArg == null) {
            return null;
        }

        x = xArg.get();
        y = yArg.get();
        z = zArg.get();

        lonlat = lonlatArg.get();

        double dnorm = Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2) + Math.pow(z, 2));

        theta.set(Math.acos(z / dnorm));
        phi.set(Math.atan2(y, x));

        if (phi.get() < 0) {
            phi.set(phi.get() + 2 * Math.PI);
        }
        if (lonlat) {
            dec.set(90 - theta.get() * 180 / Math.PI);
            ra.set(phi.get() * 180 / Math.PI);
            return Arrays.asList(ra, dec);
        } else {
            return Arrays.asList(theta, phi);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("vec2ang", children);
    }
}
