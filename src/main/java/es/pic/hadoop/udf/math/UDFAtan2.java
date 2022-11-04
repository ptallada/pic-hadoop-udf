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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@Description(
    name="atan2",
    value="_FUNC_(y, x) - Returns atan2",
    extended="SELECT _FUNC_(1.2, 2.1) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFAtan2 extends GenericUDF {

    private final Object[] result = new Object[1];
    private final DoubleWritable atanWritable = new DoubleWritable();

    private Converter[] doubleconverter = new Converter[2];
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length !=2) {
            throw new UDFArgumentLengthException("atan2() takes 2 arguments: y, x");
        }

        ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
        doubleconverter[0] = (Converter) ObjectInspectorConverters.getConverter(arguments[0], doubleOI);
        doubleconverter[1] = (Converter) ObjectInspectorConverters.getConverter(arguments[1], doubleOI);
        result[0] = atanWritable;

        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE); 
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        DoubleWritable[] argsw1 = new DoubleWritable[2];

        argsw1[0] = (DoubleWritable) doubleconverter[0].convert(arguments[0].get());
        argsw1[1] = (DoubleWritable) doubleconverter[1].convert(arguments[1].get());

        if (argsw1[0] == null || argsw1[1] == null) {
            result[0] = null;
            return result;
        }
        
        double y = argsw1[0].get();
        double x = argsw1[1].get();
        atanWritable.set(Math.atan2(y,x));
        result[0] = atanWritable;
	
        return result[0];
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "atan2(y, x)";
    }
}
