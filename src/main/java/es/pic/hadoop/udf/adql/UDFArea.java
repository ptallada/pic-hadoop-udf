package es.pic.hadoop.udf.adql;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

// @formatter:off
@Description(
    name = "area",
    value = "_FUNC_(geom) -> area:double",
    extended = "Compute the area, in square degrees, of a given geometry."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFArea extends GenericUDF {
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

    double ra;
    double dec;

    Object blob;
    ADQLGeometry geom;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 1) {
            if (!ObjectInspectorUtils.compareTypes(arguments[0], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(0, "Argument has to be of ADQL geometry type.");
            }
        } else {
            throw new UDFArgumentLengthException("This function takes a single argument: geometry");
        }

        return doubleOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        blob = arguments[0].get();

        if (blob == null) {
            return null;
        }

        geom = ADQLGeometry.fromBlob(blob);

        return new DoubleWritable(geom.area());
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("area", children);
    }
}
