package es.pic.hadoop.udf.adql;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;

// @formatter:off
@Description(
    name = "coord2",
    value = "_FUNC_(pt:point) -> dec:double",
    extended = "Returns the second coordinate (declination) of an ADQL point."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFCoord2 extends GenericUDF {
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    final static StandardUnionObjectInspector geomOI = ADQLGeometry.OI;

    Object pt;
    ADQLGeometry.Kind kind;
    DoubleWritable coord;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 1) {
            if (arguments[0] != geomOI) {
                throw new UDFArgumentTypeException(0, "Argument has to be of ADQL geometry type.");
            }
        } else {
            throw new UDFArgumentLengthException("This function takes 1 argument: point");
        }

        return doubleOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        pt = arguments[0].get();

        if (pt == null) {
            return null;
        }

        kind = ADQLGeometry.Kind.valueOfTag(geomOI.getTag(pt));

        if (kind != ADQLGeometry.Kind.POINT) {
            throw new UDFArgumentTypeException(0,
                    String.format("Provided geometry is not a POINT, but a %s.", kind.name()));
        }

        @SuppressWarnings("unchecked")
        List<DoubleWritable> coords = (List<DoubleWritable>) geomOI.getField(pt);

        return coords.get(1);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("coord2", children);
    }
}
