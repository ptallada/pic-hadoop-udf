package es.pic.hadoop.udf.adql;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import healpix.essentials.Moc;

public class UDAFUnion extends AbstractUDAFRegionResolver {

    protected GenericUDAFEvaluator getEvaluatorInstance() {
        return new UDAFRegionUnionEvaluator();
    }

    public static class UDAFRegionUnionEvaluator extends AbstractUDAFRegionEvaluator {

        @Override
        public void doMerge(RegionAggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }

            ADQLRegion region = ADQLRegion.fromBlob(partial);

            if (agg.moc == null) {
                agg.moc = new Moc(region.moc);
            } else {
                agg.moc = agg.moc.union(region.moc);
            }            
        }
    }
}