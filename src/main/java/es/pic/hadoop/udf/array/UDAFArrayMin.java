/*


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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

// @formatter:off
@Description(
    name = "array_min",
    value = "_FUNC_(array<T>) -> array<T>",
    extended = "Returns the min of a set of arrays."
)
// @formatter:on

public class UDAFArrayMin extends AbstractGenericUDAFResolver {

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
            case FLOAT:
            case DOUBLE:
                return new UDAFArrayMinEvaluator();
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

        UDAFArrayMinEvaluator eval = (UDAFArrayMinEvaluator) getEvaluator(parameters);

        eval.setIsAllColumns(info.isAllColumns());
        eval.setWindowing(info.isWindowing());
        eval.setIsDistinct(info.isDistinct());

        return eval;
    }

    @UDFType(commutative = true)
    public static class GenericUDAFArrayMinEvaluator extends GenericUDAFEvaluator {

        private transient ObjectInspector inputOI;
        private transient ObjectInspector outputOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);
            inputOI = parameters[0];
            // Copy to Java object because that saves object creation time.
            // Note that on average the number of copies is log(N) so that's not
            // very important.
            outputOI = ObjectInspectorUtils.getStandardObjectInspector(inputOI,
                    ObjectInspectorCopyOption.JAVA);
            return outputOI;
        }
*/
/** class for storing the current max value */
/*       @AggregationType(estimable = true)
       static class MinAgg extends AbstractAggregationBuffer {
           Object o;

           @Override
           public int estimate() {
               return JavaDataModel.PRIMITIVES2;
           }
       }

       @Override
       public AggregationBuffer getNewAggregationBuffer() throws HiveException {
           MinAgg result = new MinAgg();
           return result;
       }

       @Override
       public void reset(AggregationBuffer agg) throws HiveException {
           MinAgg myagg = (MinAgg) agg;
           myagg.o = null;
       }

       boolean warned = false;

       @Override
       public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
           assert (parameters.length == 1);
           merge(agg, parameters[0]);
       }

       @Override
       public Object terminatePartial(AggregationBuffer agg) throws HiveException {
           return terminate(agg);
       }

       @Override
       public void merge(AggregationBuffer agg, Object partial) throws HiveException {
           if (partial != null) {
               MinAgg myagg = (MinAgg) agg;
               int r = ObjectInspectorUtils.compare(myagg.o, outputOI, partial, inputOI,
                       new FullMapEqualComparer(), NullValueOption.MAXVALUE);
               if (myagg.o == null || r > 0) {
                   myagg.o = ObjectInspectorUtils.copyToStandardObject(partial, inputOI,
                           ObjectInspectorCopyOption.JAVA);
               }
           }
       }

       @Override
       public Object terminate(AggregationBuffer agg) throws HiveException {
           MinAgg myagg = (MinAgg) agg;
           return myagg.o;
       }

   

   public static class GenericUDAFArrayMinEvaluator
           extends GenericUDAFEvaluator {

       class ArraySumAggregationBuffer extends AbstractAggregationBuffer {
           ArrayList<T> sum;
       }

       protected ListObjectInspector listOI;
       protected PrimitiveObjectInspector elementOI;
       protected ObjectInspector resultOI;
       protected Converter elementConverter;

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

           listOI = (ListObjectInspector) parameters[0];
           elementOI = (PrimitiveObjectInspector) listOI.getListElementObjectInspector();

           switch (elementOI.getPrimitiveCategory()) {
           case BYTE:
           case SHORT:
           case INT:
           case LONG:
               elementConverter = ObjectInspectorConverters.getConverter(
                       PrimitiveObjectInspectorFactory
                               .getPrimitiveWritableObjectInspector(elementOI.getPrimitiveCategory()),
                       PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                               PrimitiveObjectInspector.PrimitiveCategory.LONG));

               resultOI = ObjectInspectorFactory.getStandardListObjectInspector(
                       PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                               PrimitiveObjectInspector.PrimitiveCategory.LONG));
               return resultOI;

           case FLOAT:
           case DOUBLE:
               elementConverter = ObjectInspectorConverters.getConverter(
                       PrimitiveObjectInspectorFactory
                               .getPrimitiveWritableObjectInspector(elementOI.getPrimitiveCategory()),
                       PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                               PrimitiveObjectInspector.PrimitiveCategory.DOUBLE));

               resultOI = ObjectInspectorFactory.getStandardListObjectInspector(
                       PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                               PrimitiveObjectInspector.PrimitiveCategory.DOUBLE));
               return resultOI;
           default:
               break;
           }

           throw new UDFArgumentTypeException(0,
                   String.format("Only integer or floating point numbers are accepted but, %s was passed.",
                           parameters[0].getTypeName()));

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
                   agg.sum.add((T) elementConverter.convert(element));
               }
           }
       }

       protected void iterateNext(AggregationBuffer buff, List array) {
           ArraySumAggregationBuffer agg = ((ArraySumAggregationBuffer) buff);

           for (int i = 0; i < array.size(); i++) {
               Object element = array.get(i);
               if (element != null) {
                   T a = (T) elementConverter.convert(element);
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

           List array = listOI.getList(parameters[0]);

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
*/
