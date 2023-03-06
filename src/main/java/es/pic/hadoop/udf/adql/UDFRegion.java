package es.pic.hadoop.udf.adql;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.io.ByteWritable;

// @formatter:off
@Description(
    name = "region",
    value = "_FUNC_(geom:ADQLGeometry, [order:tinyint=10]) -> region:ADQLGeometry",
    extended = "Construct an ADQL region with specified resolution from an arbitrary ADQLGeometry"
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFRegion extends GenericUDF {
    Converter orderConverter;

    final static ObjectInspector byteOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;

    Object geom;
    ADQLGeometry.Kind kind;

    ByteWritable orderArg;
    Byte order;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 0 || arguments.length > 2) {
            throw new UDFArgumentLengthException("This function takes 2 arguments at most: geometry, [order]");
        }

        if (arguments[0] != ADQLGeometry.OI) {
            throw new UDFArgumentTypeException(0, "First argument has to be of ADQL geometry type.");
        }

        if (arguments.length == 2) {
            orderConverter = ObjectInspectorConverters.getConverter(arguments[1], byteOI);
        } else {
            // Set NSIDE=1024 as default value
            order = null;
        }

        return ADQLGeometry.OI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        geom = arguments[0].get();

        if (geom == null) {
            return null;
        }

        if (arguments.length == 2) {
            orderArg = (ByteWritable) orderConverter.convert(arguments[1].get());

            if (orderArg == null) {
                return null;
            } else {
                order = orderArg.get();
                return ADQLGeometry.fromBlob(geom).toRegion(order.byteValue()).serialize();
            }
        } else {
            return ADQLGeometry.fromBlob(geom).toRegion().serialize();
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("region", children);
    }
}
