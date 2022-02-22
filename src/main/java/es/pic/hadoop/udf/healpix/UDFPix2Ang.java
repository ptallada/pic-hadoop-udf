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
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import healpix.essentials.HealpixProc;
import healpix.essentials.Pointing;

// @formatter:off
@Description(
    name = "pix2ang",
    value = "_FUNC_(order:tinyint, ipix:bigint, [nest:bool=False, [lonlat:bool=False]]) -> array<float>(theta/dec, phi/ra)",
    extended = "Return the angular coordinates corresponding to this pixel."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFPix2Ang extends GenericUDF {

    Converter orderConverter;
    Converter ipixConverter;
    Converter nestConverter;
    Converter lonlatConverter;

    final static ObjectInspector byteOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BYTE);
    final static ObjectInspector longOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
    final static ObjectInspector boolOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);

    ByteWritable orderArg = new ByteWritable();
    LongWritable ipixArg = new LongWritable();
    BooleanWritable nestArg = new BooleanWritable();
    BooleanWritable lonlatArg = new BooleanWritable();

    byte order;
    long ipix;
    boolean nest;
    boolean lonlat;

    DoubleWritable theta = new DoubleWritable();
    DoubleWritable phi = new DoubleWritable();
    DoubleWritable ra = new DoubleWritable();
    DoubleWritable dec = new DoubleWritable();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2 || arguments.length > 4) {
            throw new UDFArgumentLengthException(
                    "This function takes at least 2 arguments, no more than 4: order, ipix, nest, lonlat");
        }

        orderConverter = ObjectInspectorConverters.getConverter(arguments[0], byteOI);
        ipixConverter = ObjectInspectorConverters.getConverter(arguments[1], longOI);

        if (arguments.length >= 3) {
            nestConverter = ObjectInspectorConverters.getConverter(arguments[2], boolOI);
        }
        if (arguments.length == 4) {
            lonlatConverter = ObjectInspectorConverters.getConverter(arguments[3], boolOI);
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        orderArg = (ByteWritable) orderConverter.convert(arguments[0].get());
        ipixArg = (LongWritable) ipixConverter.convert(arguments[1].get());

        if (arguments.length >= 3) {
            nestArg = (BooleanWritable) nestConverter.convert(arguments[2].get());
        }
        if (arguments.length == 4) {
            lonlatArg = (BooleanWritable) lonlatConverter.convert(arguments[3].get());
        }
        if (orderArg == null || ipixArg == null) {
            return null;
        }

        order = orderArg.get();
        ipix = ipixArg.get();
        nest = nestArg.get();
        lonlat = lonlatArg.get();

        Pointing pt = new Pointing();
        try {
            if (nest == true) {
                pt = HealpixProc.pix2angNest(order, ipix);
            } else {
                pt = HealpixProc.pix2angRing(order, ipix);
            }
        } catch (Exception e) {
        }

        if (lonlat) {
            dec.set(90 - pt.theta * 180 / Math.PI);
            ra.set(pt.phi * 180 / Math.PI);
            return Arrays.asList(dec, ra);
        } else {
            theta.set(pt.theta);
            phi.set(pt.phi);
            return Arrays.asList(theta, phi);
        }
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return String.format("arguments (%d, %d, %b, %b)", order, ipix, nest, lonlat);
    }
}
