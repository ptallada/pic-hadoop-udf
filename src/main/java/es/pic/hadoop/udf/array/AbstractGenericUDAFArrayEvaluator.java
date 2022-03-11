package es.pic.hadoop.udf.array;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

public abstract class AbstractGenericUDAFArrayEvaluator<T extends Writable> extends GenericUDAFEvaluator {

    class ArrayAggregationBuffer extends AbstractAggregationBuffer {
        ArrayList<T> array;
    }

    protected ListObjectInspector inputOI;
    protected PrimitiveObjectInspector inputElementOI;

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
        assert (parameters.length == 1);
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
            outputElementOI = PrimitiveObjectInspectorFactory
                    .getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
            break;
        default:
            throw new UDFArgumentTypeException(0, String.format(
                    "Only arrays of integer or floating point numbers are accepted, but array<%s> was passed.",
                    inputOI.getTypeName()));
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(outputElementOI);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        return new ArrayAggregationBuffer();
    }

    protected void iterateFirst(AggregationBuffer buff, List array) {
        ArrayAggregationBuffer agg = ((ArrayAggregationBuffer) buff);

        agg.array = new ArrayList<T>(array.size());
        for (Object element : array) {
            if (element == null) {
                agg.array.add(null);
            } else {
                Converter inputElementConverter = ObjectInspectorConverters.getConverter(inputElementOI,
                        outputElementOI);
                agg.array.add((T) inputElementConverter.convert(element));
            }
        }
    }

    protected void iterateNext(AggregationBuffer buff, List array) {
        ArrayAggregationBuffer agg = ((ArrayAggregationBuffer) buff);

        for (int i = 0; i < array.size(); i++) {
            Object element = array.get(i);
            if (element != null) {
                Converter inputElementConverter = ObjectInspectorConverters.getConverter(inputElementOI,
                        outputElementOI);
                T a = (T) inputElementConverter.convert(element);
                T b = agg.array.get(i);
                if (b == null) {
                    agg.array.set(i, a);
                } else {
                    agg.array.set(i, combine(b, a));
                }
            }
        }
    }

    protected abstract T combine(T a, T b);

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
        ((ArrayAggregationBuffer) agg).array = null;
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
        return ((ArrayAggregationBuffer) agg).array;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
        List<Object> array = ((LazyBinaryArray) partial).getList();

        mergeInternal(agg, array);
    }

    protected void mergeInternal(AggregationBuffer buff, List array) throws HiveException {
        ArrayAggregationBuffer agg = (ArrayAggregationBuffer) buff;

        if (array == null) {
            return;
        } else if (agg.array == null) {
            iterateFirst(agg, array);
        } else if (agg.array.size() != array.size()) {
            throw new UDFArgumentException(
                    String.format("Arrays must have equal sizes, %d != %d.", agg.array.size(), array.size()));
        } else {
            iterateNext(agg, array);
        }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
        return ((ArrayAggregationBuffer) agg).array;
    }
}
