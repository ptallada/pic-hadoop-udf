package es.pic.hadoop.udf.math;

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
import org.apache.commons.math3.special.Erf;

// @formatter:off
@Description(
    name = "erfc",
    value = "_FUNC_(x) - Returns the complementary error function", 
    extended = "SELECT _FUNC_(1.2) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true, 
    stateful = false
)
// @formatter:on
public class UDFErfc extends GenericUDF {

    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

    Converter xConverter;

    DoubleWritable xArg;

    double x;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("This function takes 1 argument: x");
        }

        xConverter = ObjectInspectorConverters.getConverter(arguments[0], doubleOI);

        return doubleOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        xArg = (DoubleWritable) xConverter.convert(arguments[0].get());

        if (xArg == null) {
            return null;
        }

        x = xArg.get();

        return new DoubleWritable(Erf.erfc(x));
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "erfc(x)";
    }
}
