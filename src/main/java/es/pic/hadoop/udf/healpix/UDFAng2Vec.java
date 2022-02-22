package es.pic.hadoop.udf.healpix;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;

// @formatter:off
@Description(
    name = "ang2vec",
    value = "_FUNC_(theta/dec:float, phi/ra:float, [lonlat:bool=False]) -> array<float>(x, y, z)",
    extended = "Return the 3D position vector corresponding to these angular coordinates."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFAng2Vec extends GenericUDF {

    Converter thetaConverter;
    Converter phiConverter;
    Converter lonlatConverter;

    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
    final static ObjectInspector boolOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);

    DoubleWritable thetaArg;
    DoubleWritable phiArg;
    BooleanWritable lonlatArg = new BooleanWritable();
    // Alias
    DoubleWritable raArg;
    DoubleWritable decArg;

    double theta;
    double phi;
    boolean lonlat;

    DoubleWritable x = new DoubleWritable();
    DoubleWritable y = new DoubleWritable();
    DoubleWritable z = new DoubleWritable();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2 || arguments.length > 3) {
            throw new UDFArgumentLengthException(
                    "This function takes at least 2 arguments, no more than 3: theta/dec, phi/ra, lonlat");
        }

        thetaConverter = ObjectInspectorConverters.getConverter(arguments[0], doubleOI);
        phiConverter = ObjectInspectorConverters.getConverter(arguments[1], doubleOI);

        if (arguments.length == 3) {
            lonlatConverter = ObjectInspectorConverters.getConverter(arguments[2], boolOI);
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        thetaArg = (DoubleWritable) thetaConverter.convert(arguments[0].get());
        phiArg = (DoubleWritable) phiConverter.convert(arguments[1].get());
        // Alias
        raArg = phiArg;
        decArg = thetaArg;

        if (arguments.length == 3) {
            lonlatArg = (BooleanWritable) lonlatConverter.convert(arguments[2].get());
        }
        if (thetaArg == null || phiArg == null) {
            return null;
        }

        boolean lonlat = lonlatArg.get();

        if (lonlat) {
            theta = Math.PI / 2 - decArg.get() * Math.PI / 180;
            phi = raArg.get() * Math.PI / 180;
        } else {
            theta = thetaArg.get();
            phi = phiArg.get();
        }

        x.set(Math.sin(theta) * Math.cos(phi));
        y.set(Math.sin(theta) * Math.sin(phi));
        z.set(Math.cos(theta));

        return Arrays.asList(x, y, z);
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return String.format("arguments (%g, %g, %b)", theta, phi, lonlat);
    }
}
