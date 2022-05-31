package es.pic.hadoop.udf.array;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

// @formatter:off
@Description(
    name = "array_std_pop",
    value = "_FUNC_(array<T>) -> array<T>",
    extended = "Returns the population standard deviation of a set of arrays."
)
// @formatter:on
public class UDAFArrayStdPop extends AbstractUDAFArrayDispersionResolver {

    @Override
    protected GenericUDAFEvaluator getEvaluatorInstance() {
        return new UDAFArrayStdPopEvaluator();
    }

    @UDFType(commutative = true)
    public static class UDAFArrayStdPopEvaluator extends AbstractGenericUDAFArrayDispersionEvaluator {
        public double calculateResult(long count, double sum, double variance) {
            if (count == 1) {
                return 0;
            } else {
                return Math.sqrt(variance / count);
            }
        }
    }
}
