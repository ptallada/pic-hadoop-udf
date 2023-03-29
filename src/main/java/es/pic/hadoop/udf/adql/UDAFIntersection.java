package es.pic.hadoop.udf.adql;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public class UDAFIntersection extends AbstractUDAFRegionResolver {

    protected GenericUDAFEvaluator getEvaluatorInstance() {
        return new UDAFRegionIntersectionEvaluator();
    }

    public static class UDAFRegionIntersectionEvaluator extends AbstractUDAFRegionEvaluator {

        @Override
        public void doMerge(RegionAggregationBuffer agg, ADQLRegion region) {
            if (agg.rs == null) {
                agg.rs = new ADQLRangeSet(region.getRangeSet());
            } else {
                agg.rs = agg.rs.intersection(region.getRangeSet());
            }
        }
    }
}