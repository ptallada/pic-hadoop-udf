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

// @formatter:off
@Description(
    name = "complement",
    value = "_FUNC_(region:ADQLRegion) -> region:ADQLRegion",
    extended = "Return the complement of an ADQL region."
)
@UDFType(
    deterministic = true,
    stateful = false
)
// @formatter:on
public class UDFComplement extends GenericUDF {

    Object blob;
    ADQLGeometry geom;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 1) {
            if (!ObjectInspectorUtils.compareTypes(arguments[0], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(0, "The argument has to be of ADQL geometry type.");
            }
        } else {
            throw new UDFArgumentLengthException("This function takes 1 arguments: region");
        }

        return ADQLGeometry.OI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        blob = arguments[0].get();

        if (blob == null) {
            return null;
        }

        geom = ADQLGeometry.fromBlob(blob);

        return geom.complement().serialize();
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("complement", children);
    }
}
