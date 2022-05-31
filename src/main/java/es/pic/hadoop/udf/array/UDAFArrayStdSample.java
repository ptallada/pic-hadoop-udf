package es.pic.hadoop.udf.array;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

// @formatter:off
@Description(
    name = "array_std_sample",
    value = "_FUNC_(array<T>) -> array<T>",
    extended = "Returns the sample standard deviation of a set of arrays."
)
// @formatter:on
public class UDAFArrayStdSample extends AbstractUDAFArrayDispersionResolver {

    @Override
    protected GenericUDAFEvaluator getEvaluatorInstance() {
        return new UDAFArrayStdSampleEvaluator();
    }

    @UDFType(commutative = true)
    public static class UDAFArrayStdSampleEvaluator extends AbstractGenericUDAFArrayDispersionEvaluator {
        public double calculateResult(long count, double sum, double variance) {
            if (count == 1) {
                return 0;
            } else {
                return Math.sqrt(variance / (count - 1));
            }
        }
    }
}
