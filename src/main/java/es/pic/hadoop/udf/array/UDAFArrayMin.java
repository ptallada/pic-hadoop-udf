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
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;

// @formatter:off
@Description(
    name = "array_min",
    value = "_FUNC_(array<T>) -> array<T>",
    extended = "Returns the min of a set of arrays."
)
// @formatter:on
public class UDAFArrayMin extends AbstractUDAFArrayResolver {

    @Override
    protected GenericUDAFEvaluator getEvaluatorInstance(PrimitiveCategory category) {
        switch (category) {
        case BYTE:
            return new UDAFArrayByteMinEvaluator();
        case SHORT:
            return new UDAFArrayShortMinEvaluator();
        case INT:
            return new UDAFArrayIntMinEvaluator();
        case LONG:
            return new UDAFArrayLongMinEvaluator();
        case FLOAT:
            return new UDAFArrayFloatMinEvaluator();
        case DOUBLE:
            return new UDAFArrayDoubleMinEvaluator();
        default:
            return null;
        }
    }

    @UDFType(commutative = true)
    public static abstract class GenericUDAFArrayMinEvaluator<T extends Writable>
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

    public static class UDAFArrayByteMinEvaluator extends GenericUDAFArrayMinEvaluator<ByteWritable> {
        @Override
        protected ByteWritable doIterate(ByteWritable self, ByteWritable other) {
            if (self == null) {
                return new ByteWritable(other.get());
            } else {
                return new ByteWritable((byte) Integer.min(self.get(), other.get()));
            }
        }
    }

    public static class UDAFArrayShortMinEvaluator extends GenericUDAFArrayMinEvaluator<ShortWritable> {
        @Override
        protected ShortWritable doIterate(ShortWritable self, ShortWritable other) {
            if (self == null) {
                return new ShortWritable(other.get());
            } else {
                return new ShortWritable((short) Integer.min(self.get(), other.get()));
            }
        }
    }

    public static class UDAFArrayIntMinEvaluator extends GenericUDAFArrayMinEvaluator<IntWritable> {
        @Override
        protected IntWritable doIterate(IntWritable self, IntWritable other) {
            if (self == null) {
                return new IntWritable(other.get());
            } else {
                return new IntWritable(Integer.min(self.get(), other.get()));
            }
        }
    }

    public static class UDAFArrayLongMinEvaluator extends GenericUDAFArrayMinEvaluator<LongWritable> {
        @Override
        protected LongWritable doIterate(LongWritable self, LongWritable other) {
            if (self == null) {
                return new LongWritable(other.get());
            } else {
                return new LongWritable(Long.min(self.get(), other.get()));
            }
        }
    }

    public static class UDAFArrayFloatMinEvaluator extends GenericUDAFArrayMinEvaluator<FloatWritable> {
        @Override
        protected FloatWritable doIterate(FloatWritable self, FloatWritable other) {
            if (self == null) {
                return new FloatWritable(other.get());
            } else {
                return new FloatWritable(Float.min(self.get(), other.get()));
            }
        }
    }

    public static class UDAFArrayDoubleMinEvaluator extends GenericUDAFArrayMinEvaluator<DoubleWritable> {
        @Override
        protected DoubleWritable doIterate(DoubleWritable self, DoubleWritable other) {
            if (self == null) {
                return new DoubleWritable(other.get());
            } else {
                return new DoubleWritable(Double.min(self.get(), other.get()));
            }
        }
    }

}
