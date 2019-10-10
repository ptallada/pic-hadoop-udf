package es.pic.astro.hive_udf;

import java.lang.Math;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@Description(
    name="magnified_positions",
    value="_FUNC_(ra, dec, defl_1, defl_2) - Returns a struct with the magnified positions ra_mag and dec_mag",
    extended="SELECT _FUNC_(23.1, 56.4, 0.65, 0.12) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFMagnifiedPositions extends GenericUDF {

    private final Object[] result = new Object[2];
    private final DoubleWritable RaMagWritable = new DoubleWritable();
    private final DoubleWritable DecMagWritable = new DoubleWritable();

    private Converter[] converters = new Converter[4];

    private static double clip(double val, double min, double max) {
        return Math.max(min, Math.min(max, val));
    }
  
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 4) {
            throw new UDFArgumentLengthException("magnified_positions() takes 4 arguments: ra, dec, defl_1, defl_2");
        }

        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);

        for (int i = 0; i < arguments.length; i ++) {
			converters[i] = (Converter) ObjectInspectorConverters.getConverter(arguments[0], doubleOI);
		}

        fieldNames.add("ra_mag");
        fieldNames.add("dec_mag");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        result[0] = RaMagWritable;
        result[1] = DecMagWritable;

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        DoubleWritable argsw[] = new DoubleWritable[4];
 
        for (int i = 0; i < arguments.length; i ++) {
			argsw[i] = (DoubleWritable) converters[i].convert(arguments[i].get());
            
            if (argsw[i] == null) {
                result[0] = null;
                result[1] = null;
                
                return result;
            }
		}

        double ra = argsw[0].get();
        double dec = argsw[1].get();
        double defl_1 = argsw[2].get();
        double defl_2 = argsw[3].get();

        double phi = Math.toRadians(ra);
        double theta = Math.toRadians(90 - dec);

        double cth0 = Math.cos(theta);
        double sth0 = Math.sqrt(1 - cth0 * cth0);

        double grad_len = Math.sqrt(defl_1 * defl_1 + defl_2 * defl_2);
        double sinc_grad_len = Math.sin(grad_len)/grad_len;

        double cth = Math.cos(grad_len)*cth0 + sinc_grad_len*sth0*defl_1;
        double sth = Math.sqrt(1 - cth * cth);

        double phi_lensed = phi + Math.asin(clip(-defl_2*sinc_grad_len/sth, -1, 1));
        double theta_lensed = Math.acos(cth);

        RaMagWritable.set(Math.toDegrees(phi_lensed));
        DecMagWritable.set(90 - Math.toDegrees(theta_lensed));

        result[0] = RaMagWritable;
        result[1] = DecMagWritable;
        
        return result;
    }
  
    @Override
    public String getDisplayString(String[] arg0) {
        return "magnified_positions(ra, dec, dfl_1, dfl_2)";
    }

}
