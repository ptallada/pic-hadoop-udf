package es.pic.hadoop.udf.array;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

// @formatter:off
@Description(
    name = "array_sum",
    value = "_FUNC_(array<T>) -> array<T>",
    extended = "Returns the sum of a set of arrays."
)
// @formatter:on
public class UDAFArraySum extends AbstractUDAFArrayResolver {

    @Override
    protected GenericUDAFEvaluator getEvaluatorInstance(PrimitiveCategory category) {
        switch (category) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
            return new UDAFArrayLongSumEvaluator();
        case FLOAT:
        case DOUBLE:
            return new UDAFArrayDoubleSumEvaluator();
        default:
            return null;
        }
    }

    @UDFType(commutative = true)
    public static abstract class GenericUDAFArraySumEvaluator<T extends Writable>
            extends AbstractGenericUDAFArrayEvaluator<T> {

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
                outputElementOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
                break;
            case FLOAT:
            case DOUBLE:
                outputElementOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
                break;
            default:
                throw new UDFArgumentTypeException(0, String.format(
                        "Only arrays of integer or floating point numbers are accepted, but array<%s> was passed.",
                        inputOI.getTypeName()));
            }

            return outputOI = ObjectInspectorFactory.getStandardListObjectInspector(outputElementOI);
        }
    }

    public static class UDAFArrayLongSumEvaluator extends GenericUDAFArraySumEvaluator<LongWritable> {
        @Override
        protected LongWritable doIterate(LongWritable self, LongWritable other) {
            if (self == null) {
                return new LongWritable(other.get());
            } else {
                return new LongWritable(self.get() + other.get());
            }
        }
    }

    public static class UDAFArrayDoubleSumEvaluator extends GenericUDAFArraySumEvaluator<DoubleWritable> {
        @Override
        protected DoubleWritable doIterate(DoubleWritable self, DoubleWritable other) {
            if (self == null) {
                return new DoubleWritable(other.get());
            } else {
                return new DoubleWritable(self.get() + other.get());
            }
        }
    }
}
