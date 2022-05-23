package es.pic.hadoop.udf.array;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

// @formatter:off
@Description(
    name = "array_var_sample",
    value = "_FUNC_(array<T>) -> array<T>",
    extended = "Returns the sample variance of a set of arrays."
)
// @formatter:on
public class UDAFArrayVarianceSample extends UDAFArrayVariancePop {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentLengthException("This function takes exactly one argument: array");
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
                return new UDAFArrayVarianceSampleEvaluator();
            default:
                break;
            }
        }
        throw new UDFArgumentTypeException(0,
                String.format(
                        "Only arrays of integer or floating point numbers are accepted but, array<%s> was passed.",
                        elementTI.getTypeName()));
    }

    @UDFType(commutative = true)
    public static class UDAFArrayVarianceSampleEvaluator extends UDAFArrayVariancePopEvaluator {
        @Override
        public double calculateVarianceResult(double variance, long count) {
            return variance / (count - 1);
        }
    }
}
