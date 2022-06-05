package es.pic.hadoop.udf.array;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

// @formatter:off
@Description(
    name = "array_count",
    value = "_FUNC_(array<T>) -> array<T>",
    extended = "Returns the count of a set of arrays."
)
// @formatter:on
public class UDAFArrayCount extends AbstractUDAFArrayResolver {

    @Override
    protected GenericUDAFEvaluator getEvaluatorInstance(PrimitiveCategory category) {
        switch (category) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
            return new UDAFArrayCountEvaluator();
        default:
            return null;
        }
    }

    @UDFType(commutative = true)
    public static class UDAFArrayCountEvaluator extends AbstractGenericUDAFArrayEvaluator<LongWritable> {

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (parameters.length != 1) {
                throw new UDFArgumentLengthException(
                        String.format("A single parameter was expected, got %d instead.", parameters.length));
            }

            inputOI = (ListObjectInspector) parameters[0];
            inputElementOI = (PrimitiveObjectInspector) inputOI.getListElementObjectInspector();

            switch (inputElementOI.getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                outputElementOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
                break;
            default:
                throw new UDFArgumentTypeException(0, String.format(
                        "Only arrays of integer or floating point numbers are accepted, but array<%s> was passed.",
                        inputOI.getTypeName()));
            }

            return outputOI = ObjectInspectorFactory.getStandardListObjectInspector(outputElementOI);
        }

        @Override
        protected LongWritable doIterate(LongWritable self, LongWritable other) {
            if (self == null) {
                return new LongWritable(1);
            } else {
                return new LongWritable(self.get() + 1);
            }
        }

        @Override
        protected LongWritable doMerge(LongWritable self, LongWritable other) {
            if (self == null) {
                return new LongWritable(other.get());
            } else {
                return new LongWritable(self.get() + other.get());
            }
        }
    }
}
