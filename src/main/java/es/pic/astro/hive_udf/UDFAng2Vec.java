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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;

@Description(
    name="ang2vec",
    value="_FUNC_(x, y, lonlat) - Returns a struct with the vector of the angles",
    extended="SELECT _FUNC_(1.27, 1.34, false) FROM foo LIMIT 1;"
)
@UDFType(
    deterministic = true,
    stateful = false
)
public class UDFAng2Vec extends GenericUDF {

    private final Object[] result = new Object[3];
    private final DoubleWritable xWritable = new DoubleWritable();
    private final DoubleWritable yWritable = new DoubleWritable();
    private final DoubleWritable zWritable = new DoubleWritable();    

    private Converter[] doubleconverter = new Converter[2];
    private Converter[] booleanconverter = new Converter[1];
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2 || arguments.length > 3) {
            throw new UDFArgumentLengthException("pix2vec() takes at least 2 arguments, no more than 3: theta, phi, lonlat");
        }

        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);

        doubleconverter[0] = (Converter) ObjectInspectorConverters.getConverter(arguments[0], doubleOI);
        doubleconverter[1] = (Converter) ObjectInspectorConverters.getConverter(arguments[1], doubleOI);

        if (arguments.length == 3) {
            ObjectInspector boolOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
            booleanconverter[0] = (Converter) ObjectInspectorConverters.getConverter(arguments[2], boolOI);
        }

        fieldNames.add("x");
        fieldNames.add("y");
        fieldNames.add("z");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        result[0] = xWritable;
        result[1] = yWritable;
        result[2] = zWritable;

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        DoubleWritable[] argsw1 = new DoubleWritable[2];
        BooleanWritable argsw3 = new BooleanWritable(false);
        argsw1[0] = (DoubleWritable) doubleconverter[0].convert(arguments[0].get());
        argsw1[1] = (DoubleWritable) doubleconverter[1].convert(arguments[1].get());
        if (arguments.length == 3) {
            argsw3 = (BooleanWritable) booleanconverter[0].convert(arguments[2].get());
        }
        if (argsw1[0] == null || argsw1[1] == null){
            result[0] = null;
            result[1] = null;
            return result;
        }
        double theta = argsw1[0].get();
        double phi = argsw1[1].get();
        boolean lonlat = argsw3.get();
        if (lonlat) {
            theta = theta * Math.PI / 180;
            phi = Math.PI / 2 - phi * Math.PI / 180;
            double aux = theta;
            theta = phi;
            phi = aux;
        }
        xWritable.set(Math.sin(theta)*Math.cos(phi));
        yWritable.set(Math.sin(theta)*Math.sin(phi));
        zWritable.set(Math.cos(theta));
 
        result[0] = xWritable;
        result[1] = yWritable;
        result[2] = zWritable;

        return result;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "ang2vec(theta, phi, lonlat)";
    }
}
