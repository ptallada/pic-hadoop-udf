package es.pic.hadoop.udf.array;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
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
public class UDAFArrayMax extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentLengthException("This function takes exactly one argument: array");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(0, String.format(
                    "Only array arguments are accepted but %s was passed.", parameters[0].getTypeName()));
        }

        ListTypeInfo listTI = (ListTypeInfo) parameters[0];
        TypeInfo elementTI = listTI.getListElementTypeInfo();

        if (listTI.getListElementTypeInfo().getCategory() == ObjectInspector.Category.PRIMITIVE) {
            switch (((PrimitiveTypeInfo) elementTI).getPrimitiveCategory()) {
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
                break;
            }
        }
        throw new UDFArgumentTypeException(0, String.format(
                "Only arrays of integer or floating point numbers are accepted but, array<%s> was passed.",
                elementTI.getTypeName()));
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        if (info.isAllColumns() || info.isDistinct()) {
            throw new SemanticException("The specified syntax for UDAF invocation is invalid.");
        }

        TypeInfo[] parameters = info.getParameters();

        GenericUDAFArrayMaxEvaluator eval = (GenericUDAFArrayMaxEvaluator) getEvaluator(parameters);

        eval.setIsAllColumns(info.isAllColumns());
        eval.setWindowing(info.isWindowing());
        eval.setIsDistinct(info.isDistinct());

        return eval;
    }

    @UDFType(commutative = true)
    public static abstract class GenericUDAFArrayMaxEvaluator<T extends Writable>
            extends AbstractGenericUDAFArrayEvaluator<T> {

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

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

            return ObjectInspectorFactory.getStandardListObjectInspector(outputElementOI);
        }
    }

    public static class UDAFArrayByteMaxEvaluator extends GenericUDAFArrayMaxEvaluator<ByteWritable> {
        protected ByteWritable combine(ByteWritable a, ByteWritable b) {
            return new ByteWritable((byte) Integer.max(a.get(), b.get()));
        }
    }

    public static class UDAFArrayShortMaxEvaluator extends GenericUDAFArrayMaxEvaluator<ShortWritable> {
        protected ShortWritable combine(ShortWritable a, ShortWritable b) {
            return new ShortWritable((short) Integer.max(a.get(), b.get()));
        }
    }

    public static class UDAFArrayIntMaxEvaluator extends GenericUDAFArrayMaxEvaluator<IntWritable> {
        protected IntWritable combine(IntWritable a, IntWritable b) {
            return new IntWritable(Integer.max(a.get(), b.get()));
        }
    }

    public static class UDAFArrayLongMaxEvaluator extends GenericUDAFArrayMaxEvaluator<LongWritable> {
        protected LongWritable combine(LongWritable a, LongWritable b) {
            return new LongWritable(Long.max(a.get(), b.get()));
        }
    }

    public static class UDAFArrayFloatMaxEvaluator extends GenericUDAFArrayMaxEvaluator<FloatWritable> {
        protected FloatWritable combine(FloatWritable a, FloatWritable b) {
            return new FloatWritable(Float.max(a.get(), b.get()));
        }
    }

    public static class UDAFArrayDoubleMaxEvaluator extends GenericUDAFArrayMaxEvaluator<DoubleWritable> {
        protected DoubleWritable combine(DoubleWritable a, DoubleWritable b) {
            return new DoubleWritable(Double.max(a.get(), b.get()));
        }
    }
}
