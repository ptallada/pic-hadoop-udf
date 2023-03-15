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

// @formatter:off
@Description(
    name = "centroid",
    value = "_FUNC_(geom) -> centroid:ADQLGeometry",
    extended = "Compute the centroid of a given geometry."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFCentroid extends GenericUDF {

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
            throw new UDFArgumentLengthException("This function takes a single argument: geometry");
        }

        return ADQLGeometry.OI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        blob = arguments[0].get();

        if (blob == null) {
            return null;
        }

        geom = ADQLGeometry.fromBlob(blob, inputOI);

        return geom.centroid().serialize();
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("centroid", children);
    }
}
