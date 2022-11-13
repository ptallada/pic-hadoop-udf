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
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;

// @formatter:off
@Description(
    name = "array_max",
    value = "_FUNC_(array<T>) -> array<T>",
    extended = "Returns the max of a set of arrays."
)
// @formatter:on
public class UDAFArrayMax extends AbstractUDAFArrayResolver {

    @Override
    protected GenericUDAFEvaluator getEvaluatorInstance(PrimitiveCategory category) {
        switch (category) {
        case BYTE:
            return new UDAFArrayByteMaxEvaluator();
        case SHORT:
            return new UDAFArrayShortMaxEvaluator();
        case INT:
            return new UDAFArrayIntMaxEvaluator();
        case LONG:
            return new UDAFArrayLongMaxEvaluator();
        case FLOAT:
            return new UDAFArrayFloatMaxEvaluator();
        case DOUBLE:
            return new UDAFArrayDoubleMaxEvaluator();
        default:
            return null;
        }
    }

    @UDFType(commutative = true)
    public static abstract class GenericUDAFArrayMaxEvaluator<T extends Writable>
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
            case FLOAT:
            case DOUBLE:
                outputElementOI = PrimitiveObjectInspectorFactory
                        .getPrimitiveWritableObjectInspector(inputElementOI.getPrimitiveCategory());
                break;
            default:
                throw new UDFArgumentTypeException(0, String.format(
                        "Only arrays of integer or floating point numbers are accepted, but array<%s> was passed.",
                        inputOI.getTypeName()));
            }

            return outputOI = ObjectInspectorFactory.getStandardListObjectInspector(outputElementOI);
        }
    }

    public static class UDAFArrayByteMaxEvaluator extends GenericUDAFArrayMaxEvaluator<ByteWritable> {
        @Override
        protected ByteWritable doIterate(ByteWritable self, ByteWritable other) {
            if (self == null) {
                return new ByteWritable(other.get());
            } else {
                return new ByteWritable((byte) Integer.max(self.get(), other.get()));
            }
        }
    }

    public static class UDAFArrayShortMaxEvaluator extends GenericUDAFArrayMaxEvaluator<ShortWritable> {
        @Override
        protected ShortWritable doIterate(ShortWritable self, ShortWritable other) {
            if (self == null) {
                return new ShortWritable(other.get());
            } else {
                return new ShortWritable((short) Integer.max(self.get(), other.get()));
            }
        }
    }

    public static class UDAFArrayIntMaxEvaluator extends GenericUDAFArrayMaxEvaluator<IntWritable> {
        @Override
        protected IntWritable doIterate(IntWritable self, IntWritable other) {
            if (self == null) {
                return new IntWritable(other.get());
            } else {
                return new IntWritable(Integer.max(self.get(), other.get()));
            }
        }
    }

    public static class UDAFArrayLongMaxEvaluator extends GenericUDAFArrayMaxEvaluator<LongWritable> {
        @Override
        protected LongWritable doIterate(LongWritable self, LongWritable other) {
            if (self == null) {
                return new LongWritable(other.get());
            } else {
                return new LongWritable(Long.max(self.get(), other.get()));
            }
        }
    }

    public static class UDAFArrayFloatMaxEvaluator extends GenericUDAFArrayMaxEvaluator<FloatWritable> {
        @Override
        protected FloatWritable doIterate(FloatWritable self, FloatWritable other) {
            if (self == null) {
                return new FloatWritable(other.get());
            } else {
                return new FloatWritable(Float.max(self.get(), other.get()));
            }
        }
    }

    public static class UDAFArrayDoubleMaxEvaluator extends GenericUDAFArrayMaxEvaluator<DoubleWritable> {
        @Override
        protected DoubleWritable doIterate(DoubleWritable self, DoubleWritable other) {
            if (self == null) {
                return new DoubleWritable(other.get());
            } else {
                return new DoubleWritable(Double.max(self.get(), other.get()));
            }
        }
    }
}
