package es.pic.hadoop.udf.adql;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public class UDAFUnion extends AbstractUDAFRegionResolver {

    protected GenericUDAFEvaluator getEvaluatorInstance() {
        return new UDAFRegionUnionEvaluator();
    }

    public static class UDAFRegionUnionEvaluator extends AbstractUDAFRegionEvaluator {

        @Override
        public void doMerge(RegionAggregationBuffer agg, ADQLRegion region) {
            if (agg.rs == null) {
                agg.rs = new ADQLRangeSet(region.getRangeSet());
            } else {
                agg.rs = agg.rs.union(region.getRangeSet());
            }            
        }
    }
}