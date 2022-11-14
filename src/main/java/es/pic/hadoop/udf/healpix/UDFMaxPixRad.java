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
import org.apache.hadoop.io.ByteWritable;

import healpix.essentials.HealpixProc;

// @formatter:off
@Description(
    name = "maxpixrad",
    value = "_FUNC_(order:tinyint) -> radius:double",
    extended = "Returns, for a given resolution order, the maximum angular distance between a pixel center and its corners."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFMaxPixRad extends GenericUDF {
    Converter orderConverter;

    final static ObjectInspector byteOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

    ByteWritable orderArg;

    int order;
    double radius;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("This function takes 1 argument: order");
        }

        orderConverter = ObjectInspectorConverters.getConverter(arguments[0], byteOI);

        return doubleOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        orderArg = (ByteWritable) orderConverter.convert(arguments[0].get());

        if (orderArg == null) {
            return null;
        }

        order = orderArg.get();

        try {
            radius = HealpixProc.maxPixrad(order);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        return new DoubleWritable(radius);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("maxpixrad", children);
    }
}
