package es.pic.hadoop.udf.array;

import java.util.ArrayList;
import java.util.List;

import com.rits.cloning.Cloner;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("deprecation")
public abstract class AbstractUDAFArrayResolver extends AbstractGenericUDAFResolver {

    protected abstract GenericUDAFEvaluator getEvaluatorInstance(PrimitiveCategory category);

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentLengthException(
                    String.format("A single parameter was expected, got %d instead.", parameters.length));
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(0,
                    String.format("Only array arguments are accepted but %s was passed.", parameters[0].getTypeName()));
        }

        ListTypeInfo listTI = (ListTypeInfo) parameters[0];
        TypeInfo elementTI = listTI.getListElementTypeInfo();

        if (listTI.getListElementTypeInfo().getCategory() == ObjectInspector.Category.PRIMITIVE) {
            GenericUDAFEvaluator eval = getEvaluatorInstance(((PrimitiveTypeInfo) elementTI).getPrimitiveCategory());
            if (eval != null) {
                return eval;
            }
        }
        throw new UDFArgumentTypeException(0,
                String.format(
                        "Only arrays of integer or floating point numbers are accepted but, array<%s> was passed.",
                        elementTI.getTypeName()));
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        if (info.isAllColumns() || info.isDistinct()) {
            throw new SemanticException("The specified syntax for UDAF invocation is invalid.");
        }
        TypeInfo[] parameters = info.getParameters();

        @SuppressWarnings("unchecked")
        AbstractGenericUDAFArrayEvaluator<Writable> eval = (AbstractGenericUDAFArrayEvaluator<Writable>) getEvaluator(
                parameters);

        eval.setIsAllColumns(info.isAllColumns());
        eval.setWindowing(info.isWindowing());
        eval.setIsDistinct(info.isDistinct());

        return eval;
    }

    public abstract static class AbstractGenericUDAFArrayEvaluator<T extends Writable> extends GenericUDAFEvaluator {

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
            return super.init(m, parameters);
        }

        @Override
        public AbstractAggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new ArrayAggregationBuffer();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ArrayAggregationBuffer) agg).array = null;
        }

        protected void initAgg(ArrayAggregationBuffer agg, List<?> array) throws HiveException {
            if ((agg.array != null) && (agg.array.size() != array.size())) {
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
        public void iterate(AggregationBuffer buff, Object[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentLengthException(
                        String.format("A single parameter was expected, got %d instead.", parameters.length));
            }

            @SuppressWarnings("unchecked")
            ArrayAggregationBuffer agg = ((ArrayAggregationBuffer) buff);
            List<?> array = inputOI.getList(parameters[0]);

            if (array == null) {
                return;
            }

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
        public void merge(AggregationBuffer buff, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }

            @SuppressWarnings("unchecked")
            ArrayAggregationBuffer agg = ((ArrayAggregationBuffer) buff);
            @SuppressWarnings("unchecked")
            List<Writable> array = (List<Writable>) inputOI.getList(partial);

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
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public Object terminate(AggregationBuffer buff) throws HiveException {
            @SuppressWarnings("unchecked")
            ArrayAggregationBuffer agg = (ArrayAggregationBuffer) buff;

            if (agg.array == null) {
                return null;
            } else {
                return new Cloner().deepClone(agg.array);
            }
        }
    }
}