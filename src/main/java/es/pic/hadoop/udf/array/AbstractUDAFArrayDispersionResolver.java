package es.pic.hadoop.udf.array;

import java.util.ArrayList;
import java.util.List;

import com.rits.cloning.Cloner;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

@SuppressWarnings("deprecation")
public abstract class AbstractUDAFArrayDispersionResolver extends AbstractGenericUDAFResolver {

    protected abstract GenericUDAFEvaluator getEvaluatorInstance();

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
            switch (((PrimitiveTypeInfo) elementTI).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return getEvaluatorInstance();
            default:
                break;
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

        AbstractGenericUDAFArrayDispersionEvaluator eval = (AbstractGenericUDAFArrayDispersionEvaluator) getEvaluator(
                parameters);

        eval.setIsAllColumns(info.isAllColumns());
        eval.setWindowing(info.isWindowing());
        eval.setIsDistinct(info.isDistinct());

        return eval;
    }

    @UDFType(commutative = true)
    public abstract static class AbstractGenericUDAFArrayDispersionEvaluator extends GenericUDAFEvaluator {

        class ArrayDispersionAggregationBuffer extends AbstractAggregationBuffer {
            ArrayList<LongWritable> count;
            ArrayList<DoubleWritable> sum;
            ArrayList<DoubleWritable> var;
        }

        protected ListObjectInspector inputOI;
        protected PrimitiveObjectInspector inputElementOI;

        protected StructObjectInspector partialOI;
        protected StructField countField;
        protected StructField sumField;
        protected StructField varField;
        protected ListObjectInspector countFieldOI;
        protected ListObjectInspector sumFieldOI;
        protected ListObjectInspector varFieldOI;

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

            if (parameters.length != 1) {
                throw new UDFArgumentLengthException(
                        String.format("A single parameter was expected, got %d instead.", parameters.length));
            }

            // input
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) { // iterate() will be called
                inputOI = (ListObjectInspector) parameters[0];
                inputElementOI = (PrimitiveObjectInspector) inputOI.getListElementObjectInspector();

                switch (inputElementOI.getPrimitiveCategory()) {
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                    break;
                default:
                    throw new UDFArgumentTypeException(0, String.format(
                            "Only arrays of integer or floating point numbers are accepted but, array<%s> was passed.",
                            inputElementOI.getTypeName()));
                }
            } else { // PARTIAL2, FINAL ==> merge() will be called
                partialOI = (StructObjectInspector) parameters[0];
                List<? extends StructField> fields = partialOI.getAllStructFieldRefs();
                countField = fields.get(0);
                sumField = fields.get(1);
                varField = fields.get(2);
                countFieldOI = (ListObjectInspector) countField.getFieldObjectInspector();
                sumFieldOI = (ListObjectInspector) sumField.getFieldObjectInspector();
                varFieldOI = (ListObjectInspector) varField.getFieldObjectInspector();
            }

            // output
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) { // terminatePartial() will be called
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(ObjectInspectorFactory
                        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector)); // count
                foi.add(ObjectInspectorFactory
                        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)); // sum
                foi.add(ObjectInspectorFactory
                        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)); // var
                ArrayList<String> fname = new ArrayList<String>();
                fname.add("count");
                fname.add("sum");
                fname.add("var");
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            } else { // FINAL, COMPLETE ==> terminate() will be called()
                return ObjectInspectorFactory
                        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new ArrayDispersionAggregationBuffer();
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ArrayDispersionAggregationBuffer) agg).count = null;
            ((ArrayDispersionAggregationBuffer) agg).sum = null;
            ((ArrayDispersionAggregationBuffer) agg).var = null;
        }

        protected void initAgg(ArrayDispersionAggregationBuffer agg, int size) throws HiveException {
            if ((agg.count != null) && (agg.count.size() != size)) {
                throw new UDFArgumentException(
                        String.format("Arrays must have equal sizes, %d != %d.", agg.count.size(), size));
            }

            if (agg.count == null) {
                agg.count = new ArrayList<LongWritable>(size);
                agg.sum = new ArrayList<DoubleWritable>(size);
                agg.var = new ArrayList<DoubleWritable>(size);
                for (int i = 0; i < size; i++) {
                    agg.count.add(new LongWritable(0));
                    agg.sum.add(new DoubleWritable(0));
                    agg.var.add(new DoubleWritable(0));
                }
            }
        }

        @Override
        public void iterate(AggregationBuffer buff, Object[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentLengthException(
                        String.format("A single parameter was expected, got %d instead.", parameters.length));
            }

            List<?> array = inputOI.getList(parameters[0]);
            ArrayDispersionAggregationBuffer agg = (ArrayDispersionAggregationBuffer) buff;

            if (array == null) {
                return;
            }

            initAgg(agg, array.size());

            for (int i = 0; i < array.size(); i++) {
                Object element = array.get(i);
                if (element != null) {
                    double other = PrimitiveObjectInspectorUtils.getDouble(element, inputElementOI);

                    LongWritable count = agg.count.get(i);
                    DoubleWritable sum = agg.sum.get(i);
                    DoubleWritable var = agg.var.get(i);

                    count.set(count.get() + 1);
                    sum.set(sum.get() + other);

                    if (count.get() > 1) {
                        double t = count.get() * other - sum.get();
                        var.set(var.get() + (t * t) / (count.get() * (count.get() - 1)));
                    }
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer buff) throws HiveException {
            ArrayDispersionAggregationBuffer agg = (ArrayDispersionAggregationBuffer) buff;

            if (agg.count == null) {
                return new Object[] {
                        null, null, null // count, sum, var
                };
            } else {
                // Need to deep copy Writable counters
                ArrayList<LongWritable> count = new ArrayList<LongWritable>(agg.count.size());
                ArrayList<DoubleWritable> sum = new ArrayList<DoubleWritable>(agg.sum.size());
                ArrayList<DoubleWritable> var = new ArrayList<DoubleWritable>(agg.var.size());

                for (int i = 0; i < agg.count.size(); i++) {
                    count.add(new LongWritable(agg.count.get(i).get()));
                    sum.add(new DoubleWritable(agg.sum.get(i).get()));
                    var.add(new DoubleWritable(agg.var.get(i).get()));
                }

                return new Object[] {
                        count, sum, var
                };
            }
        }

        @Override
        public void merge(AggregationBuffer buff, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }

            ArrayDispersionAggregationBuffer agg = (ArrayDispersionAggregationBuffer) buff;

            @SuppressWarnings("unchecked")
            List<LongWritable> partial_count = (List<LongWritable>) countFieldOI
                    .getList(partialOI.getStructFieldData(partial, countField));
            @SuppressWarnings("unchecked")
            List<DoubleWritable> partial_sum = (List<DoubleWritable>) sumFieldOI
                    .getList(partialOI.getStructFieldData(partial, sumField));
            @SuppressWarnings("unchecked")
            List<DoubleWritable> partial_var = (List<DoubleWritable>) varFieldOI
                    .getList(partialOI.getStructFieldData(partial, varField));

            if (partial_count == null) {
                return;
            } else if (agg.count == null) {
                Cloner cloner = new Cloner();

                agg.count = new ArrayList<LongWritable>(cloner.deepClone(partial_count));
                agg.sum = new ArrayList<DoubleWritable>(cloner.deepClone(partial_sum));
                agg.var = new ArrayList<DoubleWritable>(cloner.deepClone(partial_var));
            } else {
                for (int i = 0; i < partial_count.size(); i++) {
                    LongWritable count = agg.count.get(i);
                    DoubleWritable sum = agg.sum.get(i);
                    DoubleWritable var = agg.var.get(i);

                    long other_count = partial_count.get(i).get();
                    double other_sum = partial_sum.get(i).get();
                    double other_var = partial_var.get(i).get();

                    if ((count.get() > 0) && (other_count > 0)) {
                        double t = (other_count / count.get()) * sum.get() - other_sum;
                        var.set(var.get() + other_var
                                + ((count.get() / other_count) / (count.get() + other_count)) * t * t);
                    } else if (count.get() == 0) {
                        var.set(other_var);
                    }

                    count.set(count.get() + other_count);
                    sum.set(sum.get() + other_sum);
                }
            }
        }

        public abstract double calculateResult(long count, double sum, double variance);

        @Override
        public Object terminate(AggregationBuffer buff) throws HiveException {
            ArrayList<DoubleWritable> res = new ArrayList<DoubleWritable>();
            ArrayDispersionAggregationBuffer agg = (ArrayDispersionAggregationBuffer) buff;

            if (agg.count == null) {
                return null;
            }

            for (int i = 0; i < agg.count.size(); i++) {
                LongWritable count = agg.count.get(i);
                DoubleWritable sum = agg.sum.get(i);
                DoubleWritable var = agg.var.get(i);

                if (count.get() == 0) {
                    res.add(null);
                } else {
                    res.add(new DoubleWritable(calculateResult(count.get(), sum.get(), var.get())));
                }
            }
            return res;
        }
    }
}
