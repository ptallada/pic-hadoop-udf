package es.pic.hadoop.udf.healpix;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
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
    name = "vec2pix",
    value = "_FUNC_(order:tinyint, x:float, y:float, z:float, [nest:bool=False]) -> ipix:bigint",
    extended = "Return the pixel corresponding to this 3D position vector."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFVec2Pix extends GenericUDF {
    Converter orderConverter;
    Converter xConverter;
    Converter yConverter;
    Converter zConverter;
    Converter nestConverter;

    final static ObjectInspector byteOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BYTE);
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
    final static ObjectInspector boolOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
    final static ObjectInspector longOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);

    ByteWritable orderArg;
    DoubleWritable xArg;
    DoubleWritable yArg;
    DoubleWritable zArg;
    BooleanWritable nestArg = new BooleanWritable();

    byte order;
    double x;
    double y;
    double z;
    boolean nest;

    long ipix;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 4 || arguments.length > 5) {
            throw new UDFArgumentLengthException(
                    "This function takes at least 4 arguments, no more than 5: order, x, y, z, nest");
        }

        orderConverter = ObjectInspectorConverters.getConverter(arguments[0], byteOI);
        xConverter = ObjectInspectorConverters.getConverter(arguments[1], doubleOI);
        yConverter = ObjectInspectorConverters.getConverter(arguments[2], doubleOI);
        zConverter = ObjectInspectorConverters.getConverter(arguments[3], doubleOI);

        if (arguments.length == 5) {
            nestConverter = ObjectInspectorConverters.getConverter(arguments[4], boolOI);
        }

        return longOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        orderArg = (ByteWritable) orderConverter.convert(arguments[0].get());
        xArg = (DoubleWritable) xConverter.convert(arguments[1].get());
        yArg = (DoubleWritable) yConverter.convert(arguments[2].get());
        zArg = (DoubleWritable) zConverter.convert(arguments[3].get());

        if (arguments.length == 5) {
            nestArg = (BooleanWritable) nestConverter.convert(arguments[4].get());
        }
        if (xArg == null || yArg == null || zArg == null) {
            return null;
        }

        order = orderArg.get();
        x = xArg.get();
        y = yArg.get();
        z = zArg.get();

        nest = nestArg.get();

        try {
            if (nest) {
                ipix = HealpixProc.vec2pixNest(order, new Vec3(x, y, z));
            } else {
                ipix = HealpixProc.vec2pixRing(order, new Vec3(x, y, z));
            }
        } catch (Exception e) {
            throw new HiveException(e);
        }

        return new LongWritable(ipix);
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return String.format("arguments (%d, %g, %g, %g, %b)", order, x, y, z, nest);
    }
}
