package es.pic.hadoop.udf.array;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

// @formatter:off
@Description(
    name = "array_var_sample",
    value = "_FUNC_(array<T>) -> array<T>",
    extended = "Returns the sample variance of a set of arrays."
)
// @formatter:on
public class UDAFArrayVarianceSample extends AbstractUDAFArrayDispersionResolver {

    @Override
    protected GenericUDAFEvaluator getEvaluatorInstance() {
        return new UDAFArrayVarianceSampleEvaluator();
    }

    @UDFType(commutative = true)
    public static class UDAFArrayVarianceSampleEvaluator extends AbstractGenericUDAFArrayDispersionEvaluator {
        public double calculateResult(long count, double sum, double variance) {
            if (count == 1) {
                return 0;
            } else {
                return variance / (count - 1);
            }
        }
    }
}
