package es.pic.hadoop.udf.adql;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

// @formatter:off
@Description(
    name = "coord1",
    value = "_FUNC_(pt:point) -> ra:double",
    extended = "Returns the first coordinate (right ascension) of an ADQL point."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFCoord1 extends GenericUDF {
    final static ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    
    StructObjectInspector inputOI;

    Object blob;
    ADQLGeometry geom;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 1) {
            
            if (!ObjectInspectorUtils.compareTypes(arguments[0], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(0, "Argument has to be of ADQL geometry type.");
            }
            inputOI = (StructObjectInspector) arguments[0];
        } else {
            throw new UDFArgumentLengthException("This function takes 1 argument: point");
        }

        return doubleOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        blob = arguments[0].get();

        if (blob == null) {
            return null;
        }

        geom = ADQLGeometry.fromBlob(blob, inputOI);

        if (!(geom instanceof ADQLPoint)) {
            throw new UDFArgumentTypeException(0,
                    String.format("Provided geometry is not a POINT, but a %s.", geom.getKind().name()));
        }

        return geom.getCoord(0);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("coord1", children);
    }
}
