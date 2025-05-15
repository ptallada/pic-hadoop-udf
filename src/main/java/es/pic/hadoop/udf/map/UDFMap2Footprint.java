package es.pic.hadoop.udf.map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import es.pic.hadoop.udf.adql.ADQLGeometry;
import es.pic.hadoop.udf.adql.ADQLRangeSet;
import es.pic.hadoop.udf.adql.ADQLRegion;
import java.util.Arrays;
import java.util.List;

@Description(
    name = "Map2Footprint",
    value = "_FUNC_(array<struct>, float) - Extracts (lo, hi) pairs up to a cumulative threshold of 'value' field",
    extended = "Example: SELECT Map2Footprint(array(named_struct('lo', 0L, 'hi', 134L, 'value', cast(0.5 AS FLOAT) ),"
             + "named_struct('lo', 134L, 'hi', 567L, 'value', cast(0.4 AS FLOAT) ),"
             + "named_struct('lo', 567L, 'hi', 789L, 'value', cast(0.1 AS FLOAT) )), 0.9);"
             + "Returns: ADQLRegion: {tag:3,coords:null,rs:{[0,134,567]}}"
)
public class UDFMap2Footprint extends GenericUDF {

    private ListObjectInspector inputOI;
    private StructObjectInspector structOI;
    private StructField valueField;
    private StructField loField;
    private StructField hiField;
    private static final PrimitiveObjectInspector longOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    private static final PrimitiveObjectInspector floatOI = PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
    private Converter thresholdConverter;
    private Converter loConverter;
    private Converter hiConverter;
    private Converter valueConverter;

    public static final StandardListObjectInspector mapOI = ObjectInspectorFactory.getStandardListObjectInspector(
            ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList("lo", "hi", "value"),
                    Arrays.asList(longOI, longOI, floatOI)));

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "this function takes exactly two arguments: a MOC map and a float threshold.");
        }

        thresholdConverter = ObjectInspectorConverters.getConverter(arguments[1], floatOI);

        // Check if the input struct matches the expected format
        if (!ObjectInspectorUtils.compareTypes(arguments[0], mapOI)) {
            throw new UDFArgumentTypeException(0,
                    "Expected an ARRAY of STRUCT<'lo':long, 'hi':long, 'value':float> as first argument.");
        }

        inputOI = (ListObjectInspector) arguments[0];

        structOI = (StructObjectInspector) inputOI.getListElementObjectInspector();
        valueField = structOI.getStructFieldRef("value");
        loField = structOI.getStructFieldRef("lo");
        hiField = structOI.getStructFieldRef("hi");

        loConverter = ObjectInspectorConverters.getConverter(loField.getFieldObjectInspector(),
                PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        hiConverter = ObjectInspectorConverters.getConverter(hiField.getFieldObjectInspector(),
                PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        valueConverter = ObjectInspectorConverters.getConverter(valueField.getFieldObjectInspector(),
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector);

        return ADQLGeometry.OI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object mapArg = arguments[0].get();
        FloatWritable thresholdArg = (FloatWritable) thresholdConverter.convert(arguments[1].get());

        if (mapArg == null || thresholdArg == null) {
            return null;
        }

        float threshold = thresholdArg.get();

        List<?> structList = inputOI.getList(mapArg);
        if (structList == null || structList.isEmpty()) {
            return null;
        }

        ADQLRangeSet rangeSet = new ADQLRangeSet();
        float cumulativeSum = 0;

        for (Object structObj : structList) {
            if (cumulativeSum >= threshold) {
                break; // Stop processing once threshold is met
            }

            if (structObj == null)
                continue;

            FloatWritable valueObj = (FloatWritable) valueConverter
                    .convert(structOI.getStructFieldData(structObj, valueField));
            LongWritable loObj = (LongWritable) loConverter.convert(structOI.getStructFieldData(structObj, loField));
            LongWritable hiObj = (LongWritable) hiConverter.convert(structOI.getStructFieldData(structObj, hiField));

            if (valueObj != null && loObj != null && hiObj != null) {
                float value = valueObj.get();
                long lo = loObj.get();
                long hi = hiObj.get();

                rangeSet.add(lo, hi); // Append the range to the set
                cumulativeSum += value;
            }
        }

        return new ADQLRegion(rangeSet).serialize();
    }

    @Override
    public String getDisplayString(String[] children) {
        return "map2footprint(" + children[0] + ", " + children[1] + ")";
    }
}
