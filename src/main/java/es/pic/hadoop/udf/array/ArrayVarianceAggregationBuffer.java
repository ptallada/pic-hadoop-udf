
package es.pic.hadoop.udf.array;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

class ArrayVarianceAggregationBuffer extends AbstractAggregationBuffer {
    ArrayList<LongWritable> count;
    ArrayList<DoubleWritable> sum;
    ArrayList<DoubleWritable> var;
}
