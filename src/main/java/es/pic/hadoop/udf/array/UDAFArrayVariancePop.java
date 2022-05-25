package es.pic.hadoop.udf.array;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

// @formatter:off
@Description(
    name = "array_var_pop",
    value = "_FUNC_(array<T>) -> array<T>",
    extended = "Returns the population variance of a set of arrays."
)
// @formatter:on
public class UDAFArrayVariancePop extends AbstractUDAFArrayDispersion {

    @Override
    protected GenericUDAFEvaluator getEvaluatorInstance() {
        return new UDAFArrayVariancePopEvaluator();
    }

    @UDFType(commutative = true)
    public static class UDAFArrayVariancePopEvaluator extends AbstractGenericUDAFArrayDispersionEvaluator {
        public double calculateVarianceResult(double variance, long count) {
            return variance / count;
        }
    }
}
