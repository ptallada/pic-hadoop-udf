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
import healpix.essentials.Vec3;

// @formatter:off
@Description(
    name = "pix2vec",
    value = "_FUNC_(order:tinyint, ipix:bigint, [nest:bool=False]) -> array<float>(x, y, z)",
    extended = "Return the 3D position vector corresponding to this pixel."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFPix2Vec extends GenericUDF {
    Converter orderConverter;
    Converter ipixConverter;
    Converter nestConverter;

    final static ObjectInspector byteOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BYTE);
    final static ObjectInspector longOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
    final static ObjectInspector boolOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);

    ByteWritable orderArg;
    LongWritable ipixArg;
    BooleanWritable nestArg = new BooleanWritable();

    byte order;
    long ipix;
    boolean nest;

    DoubleWritable x = new DoubleWritable();
    DoubleWritable y = new DoubleWritable();
    DoubleWritable z = new DoubleWritable();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2 || arguments.length > 3) {
            throw new UDFArgumentLengthException(
                    "This function takes at least 2 arguments, no more than 3: order, ipix, nest");
        }

        orderConverter = ObjectInspectorConverters.getConverter(arguments[0], byteOI);
        ipixConverter = ObjectInspectorConverters.getConverter(arguments[1], longOI);

        if (arguments.length == 3) {
            nestConverter = ObjectInspectorConverters.getConverter(arguments[2], boolOI);
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        orderArg = (ByteWritable) orderConverter.convert(arguments[0].get());
        ipixArg = (LongWritable) ipixConverter.convert(arguments[1].get());

        if (arguments.length == 3) {
            nestArg = (BooleanWritable) nestConverter.convert(arguments[2].get());
        }
        if (orderArg == null || ipixArg == null) {
            return null;
        }

        order = orderArg.get();
        ipix = ipixArg.get();
        nest = nestArg.get();

        Vec3 vec = new Vec3();
        try {
            if (nest == true) {
                vec = HealpixProc.pix2vecNest(order, ipix);
            } else {
                vec = HealpixProc.pix2vecRing(order, ipix);
            }
        } catch (Exception e) {
        }

        x.set(vec.x);
        y.set(vec.y);
        z.set(vec.z);

        return Arrays.asList(x, y, z);
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return String.format("arguments (%d, %d, %b)", order, ipix, nest);
    }
}
