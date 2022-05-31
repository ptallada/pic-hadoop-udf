package es.pic.hadoop.udf.array;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

// @formatter:off
@Description(
    name = "array_avg",
    value = "_FUNC_(array<T>) -> array<T>",
    extended = "Returns the average of a set of arrays."
)
// @formatter:on
public class UDAFArrayAverage extends AbstractUDAFArrayDispersionResolver {

    @Override
    protected GenericUDAFEvaluator getEvaluatorInstance() {
        return new UDAFArrayAverageEvaluator();
    }

    @UDFType(commutative = true)
    public static class UDAFArrayAverageEvaluator extends AbstractGenericUDAFArrayDispersionEvaluator {
        public double calculateResult(long count, double sum, double variance) {
            return sum / count;
        }
    }
}
