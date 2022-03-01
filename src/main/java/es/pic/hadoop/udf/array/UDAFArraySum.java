package es.pic.hadoop.udf.array;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

// @formatter:off
@Description(
    name = "array_sum",
    value = "_FUNC_(array<T>) -> array<T>",
    extended = "Returns the sum of a set of arrays."
)
// @formatter:on
public class UDAFArraySum extends AbstractGenericUDAFResolver {

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
            case SHORT:
            case INT:
            case LONG:
                return new UDAFArrayLongSumEvaluator();
            case FLOAT:
            case DOUBLE:
                return new UDAFArrayDoubleSumEvaluator();
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

        GenericUDAFArraySumEvaluator eval = (GenericUDAFArraySumEvaluator) getEvaluator(parameters);

        eval.setIsAllColumns(info.isAllColumns());
        eval.setWindowing(info.isWindowing());
        eval.setIsDistinct(info.isDistinct());

        return eval;
    }

    @UDFType(commutative = true)
    public static abstract class GenericUDAFArraySumEvaluator<T extends Writable>
            extends GenericUDAFEvaluator {

        class ArraySumAggregationBuffer extends AbstractAggregationBuffer {
            ArrayList<T> sum;
        }

        protected ListObjectInspector inputOI;
        protected PrimitiveObjectInspector inputElementOI;
        protected Converter inputElementConverter;

        protected ListObjectInspector outputOI;
        protected PrimitiveObjectInspector outputElementOI;

        protected boolean isAllColumns;
        protected boolean isDistinct;
        protected boolean isWindowing;

        public void setIsAllColumns(boolean isAllColumns) {
            this.isAllColumns = isAllColumns;
        }

        public void setIsDistinct(boolean isDistinct) {
            this.isDistinct = isDistinct;
        }

        public void setWindowing(boolean isWindowing) {
            this.isWindowing = isWindowing;
        }

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
                outputElementOI = PrimitiveObjectInspectorFactory
                        .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
                break;
            case FLOAT:
            case DOUBLE:
                outputElementOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                        PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
                break;
            default:
                throw new UDFArgumentTypeException(0, String.format(
                        "Only arrays of integer or floating point numbers are accepted, but array<%s> was passed.",
                        inputOI.getTypeName()));
            }

            inputElementConverter = ObjectInspectorConverters.getConverter(inputElementOI, outputElementOI);

            return ObjectInspectorFactory.getStandardListObjectInspector(outputElementOI);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new ArraySumAggregationBuffer();
        }

        protected void iterateFirst(AggregationBuffer buff, List array) {
            ArraySumAggregationBuffer agg = ((ArraySumAggregationBuffer) buff);

            agg.sum = new ArrayList<T>(array.size());
            for (Object element : array) {
                if (element == null) {
                    agg.sum.add(null);
                } else {
                    agg.sum.add((T) inputElementConverter.convert(element));
                }
            }
        }

        protected void iterateNext(AggregationBuffer buff, List array) {
            ArraySumAggregationBuffer agg = ((ArraySumAggregationBuffer) buff);

            for (int i = 0; i < array.size(); i++) {
                Object element = array.get(i);
                if (element != null) {
                    T a = (T) inputElementConverter.convert(element);
                    T b = agg.sum.get(i);
                    if (b == null) {
                        agg.sum.set(i, a);
                    } else {
                        agg.sum.set(i, combine(b, a));
                    }
                }
            }
        }

        protected abstract T combine(T a, T b);

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ArraySumAggregationBuffer) agg).sum = null;
        }

        @Override
        public void iterate(AggregationBuffer buff, Object[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentLengthException("This function takes exactly one argument: array");
            }

            List array = inputOI.getList(parameters[0]);

            mergeInternal(buff, array);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return ((ArraySumAggregationBuffer) agg).sum;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            List<Object> array = ((LazyBinaryArray) partial).getList();

            mergeInternal(agg, array);
        }

        protected void mergeInternal(AggregationBuffer buff, List array) throws HiveException {
            ArraySumAggregationBuffer agg = (ArraySumAggregationBuffer) buff;

            if (array == null) {
                return;
            } else if (agg.sum == null) {
                iterateFirst(agg, array);
            } else if (agg.sum.size() != array.size()) {
                throw new UDFArgumentException(String.format("Arrays must have equal sizes, %d != %d.",
                        agg.sum.size(), array.size()));
            } else {
                iterateNext(agg, array);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            return ((ArraySumAggregationBuffer) agg).sum;
        }
    }

    public static class UDAFArrayLongSumEvaluator extends GenericUDAFArraySumEvaluator<LongWritable> {
        protected LongWritable combine(LongWritable a, LongWritable b) {
            return new LongWritable(a.get() + b.get());
        }
    }

    public static class UDAFArrayDoubleSumEvaluator extends GenericUDAFArraySumEvaluator<DoubleWritable> {
        protected DoubleWritable combine(DoubleWritable a, DoubleWritable b) {
            return new DoubleWritable(a.get() + b.get());
        }
    }
}
