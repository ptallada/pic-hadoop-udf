package es.pic.hadoop.udf.array;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
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
            throw new UDFArgumentTypeException(0,
                    String.format(
                            "Only arrays of integer or floating point numbers are accepted, but array<%s> was passed.",
                            inputOI.getTypeName()));
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(outputElementOI);
    }

    @Override
    public AbstractAggregationBuffer getNewAggregationBuffer() throws HiveException {
        return new ArrayAggregationBuffer();
    }

    @Override
    @SuppressWarnings({
            "unchecked", "deprecation"
    })
    public void reset(AggregationBuffer agg) throws HiveException {
        ((ArrayAggregationBuffer) agg).array = null;
    }

    protected void initAgg(ArrayAggregationBuffer agg, List<?> array) throws HiveException {
        if (array == null) {
            return;
        } else if ((agg.array != null) && (agg.array.size() != array.size())) {
            throw new UDFArgumentException(
                    String.format("Arrays must have equal sizes, %d != %d.", agg.array.size(), array.size()));
        }

        // Initialize array of nulls
        if (agg.array == null) {
            agg.array = new ArrayList<T>(array.size());
            for (int i = 0; i < array.size(); i++) {
                agg.array.add(null);
            }
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public void iterate(AggregationBuffer buff, Object[] parameters) throws HiveException {
        if (parameters.length != 1) {
            throw new UDFArgumentLengthException("This function takes exactly one argument: array");
        }

        @SuppressWarnings("unchecked")
        ArrayAggregationBuffer agg = ((ArrayAggregationBuffer) buff);
        List<?> array = inputOI.getList(parameters[0]);

        initAgg(agg, array);

        for (int i = 0; i < array.size(); i++) {
            Object element = array.get(i);
            if (element != null) {
                Converter inputElementConverter = ObjectInspectorConverters.getConverter(inputElementOI,
                        outputElementOI);
                @SuppressWarnings("unchecked")
                T other = (T) inputElementConverter.convert(element);
                T self = agg.array.get(i);
                agg.array.set(i, doIterate(self, other));
            }
        }
    }

    protected abstract T doIterate(T self, T other);

    @Override
    @SuppressWarnings("deprecation")
    public void merge(AggregationBuffer buff, Object partial) throws HiveException {
        if (partial == null) {
            return;
        }

        @SuppressWarnings("unchecked")
        ArrayAggregationBuffer agg = ((ArrayAggregationBuffer) buff);
        @SuppressWarnings("unchecked")
        List<Writable> array = (List<Writable>) outputOI.getList(partial);

        initAgg(agg, array);

        for (int i = 0; i < array.size(); i++) {
            Object element = array.get(i);
            if (element != null) {
                @SuppressWarnings("unchecked")
                T other = (T) element;
                T self = agg.array.get(i);
                agg.array.set(i, doMerge(self, other));
            }
        }
    }

    protected T doMerge(T self, T other) {
        return doIterate(self, other);
    }

    @Override
    @SuppressWarnings({
            "unchecked", "deprecation"
    })
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
        return ((ArrayAggregationBuffer) agg).array;
    }

    @Override
    @SuppressWarnings({
            "unchecked", "deprecation"
    })
    public Object terminate(AggregationBuffer agg) throws HiveException {
        return ((ArrayAggregationBuffer) agg).array;
    }
}
