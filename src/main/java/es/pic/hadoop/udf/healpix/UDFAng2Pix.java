package es.pic.hadoop.udf.healpix;

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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;

import healpix.essentials.HealpixProc;
import healpix.essentials.Pointing;

// @formatter:off
@Description(
    name = "ang2pix",
    value = "_FUNC_(order:tinyint, theta/ra:float, phi/dec:float, [nest:bool=False, [lonlat:bool=False]]) -> ipix:bigint",
    extended = "Return the pixel corresponding to these angular coordinates."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFAng2Pix extends GenericUDF {
    Converter orderConverter;
    Converter thetaConverter;
    Converter phiConverter;
    Converter nestConverter;
    Converter lonlatConverter;

    final static ObjectInspector byteOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    final static ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    final static ObjectInspector longOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;

    ByteWritable orderArg;
    DoubleWritable thetaArg;
    DoubleWritable phiArg;
    BooleanWritable nestArg = new BooleanWritable();
    BooleanWritable lonlatArg = new BooleanWritable();
    // Alias
    DoubleWritable raArg;
    DoubleWritable decArg;

    byte order;
    double theta;
    double phi;
    boolean nest;
    boolean lonlat;

    long ipix;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 3 || arguments.length > 5) {
            throw new UDFArgumentLengthException(
                    "This function takes at least 3 arguments, no more than 5: order, theta/ra, phi/dec, nest, lonlat");
        }

        orderConverter = ObjectInspectorConverters.getConverter(arguments[0], byteOI);
        thetaConverter = ObjectInspectorConverters.getConverter(arguments[1], doubleOI);
        phiConverter = ObjectInspectorConverters.getConverter(arguments[2], doubleOI);

        if (arguments.length >= 4) {
            nestConverter = ObjectInspectorConverters.getConverter(arguments[3], boolOI);
        }
        if (arguments.length >= 5) {
            lonlatConverter = ObjectInspectorConverters.getConverter(arguments[4], boolOI);
        }

        return longOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        orderArg = (ByteWritable) orderConverter.convert(arguments[0].get());
        thetaArg = (DoubleWritable) thetaConverter.convert(arguments[1].get());
        phiArg = (DoubleWritable) phiConverter.convert(arguments[2].get());
        // Alias
        raArg = thetaArg;
        decArg = phiArg;

        if (arguments.length >= 4) {
            nestArg = (BooleanWritable) nestConverter.convert(arguments[3].get());
        }
        if (arguments.length == 5) {
            lonlatArg = (BooleanWritable) lonlatConverter.convert(arguments[4].get());
        }
        if (orderArg == null || thetaArg == null || phiArg == null) {
            return null;
        }

        order = orderArg.get();
        nest = nestArg.get();
        lonlat = lonlatArg.get();

        if (lonlat) {
            theta = Math.PI / 2 - decArg.get() * Math.PI / 180;
            phi = raArg.get() * Math.PI / 180;
        } else {
            theta = thetaArg.get();
            phi = phiArg.get();
        }

        try {
            if (nest) {
                ipix = HealpixProc.ang2pixNest(order, new Pointing(theta, phi));
            } else {
                ipix = HealpixProc.ang2pixRing(order, new Pointing(theta, phi));
            }
        } catch (Exception e) {
            throw new HiveException(e);
        }

        return new LongWritable(ipix);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("ang2pix", children);
    }
}
