package es.pic.hadoop.udf.misc;

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
    name="mw_theta",
    value="_FUNC_(x) - return x to Mollweide projection ",
    extended="SELECT _FUNC_(10) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFMWtheta extends GenericUDF {

    private final Object[] result = new Object[1];
    private final DoubleWritable degWritable = new DoubleWritable();

    private Converter doubleconverter;
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length !=1) {
            throw new UDFArgumentLengthException("pix2ang() takes 1 arguments: x");
        }

        ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
        doubleconverter = (Converter) ObjectInspectorConverters.getConverter(arguments[0], doubleOI);
        result[0] = degWritable;

        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE); 
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        DoubleWritable argsw1 = new DoubleWritable();

        argsw1 = (DoubleWritable) doubleconverter.convert(arguments[0].get());

        if (argsw1 == null) {
            result[0] = null;
            return result;
        }
        
        double x = argsw1.get();
        double fi;
        double t;
        double t0;
        double eps;

        if (Math.abs((Math.abs(x)-90.)/90.) < .00001) {
            degWritable.set(x);
            result[0] = degWritable;
            return result[0];
        }

        fi = Math.toRadians(x);
        t = fi;
        eps = 1.;

        while (eps > .00001) {
            t0 = t;
            t -= (2.*t + Math.sin(2.*t) - Math.PI*Math.sin(fi))/(2. + 2.*Math.cos(2.*t));
            eps = Math.abs((t-t0)/t0);
        }

        degWritable.set(Math.toDegrees(t));
        result[0] = degWritable;
	
        return result[0];
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "mw_theta(x)";
    }
}
