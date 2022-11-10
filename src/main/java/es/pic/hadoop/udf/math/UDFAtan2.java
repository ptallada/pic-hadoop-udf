package es.pic.hadoop.udf.math;

import java.lang.Math;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;

// @formatter:off
@Description(
    name = "atan2",
    value = "_FUNC_(y, x) - Returns atan2", 
    extended = "SELECT _FUNC_(1.2, 2.1) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true, 
    stateful = false
)
// @formatter:on
public class UDFAtan2 extends GenericUDF {

    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

    Converter xConverter;
    Converter yConverter;

    DoubleWritable xArg;
    DoubleWritable yArg;

    double x;
    double y;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("This function takes 2 arguments: y, x");
        }

        xConverter = ObjectInspectorConverters.getConverter(arguments[1], doubleOI);
        yConverter = ObjectInspectorConverters.getConverter(arguments[0], doubleOI);

        return doubleOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        xArg = (DoubleWritable) xConverter.convert(arguments[1].get());
        yArg = (DoubleWritable) yConverter.convert(arguments[0].get());

        if (xArg == null || yArg == null) {
            return null;
        }

        x = xArg.get();
        y = yArg.get();

        return new DoubleWritable(Math.atan2(y, x));
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "atan2(y, x)";
    }
}
