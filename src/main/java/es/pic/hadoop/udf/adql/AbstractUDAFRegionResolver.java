package es.pic.hadoop.udf.adql;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@SuppressWarnings("deprecation")
public abstract class AbstractUDAFRegionResolver extends AbstractGenericUDAFResolver {

    protected abstract GenericUDAFEvaluator getEvaluatorInstance();

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentLengthException(
                    String.format("A single parameter was expected, got %d instead.", parameters.length));
        }

        return getEvaluatorInstance();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        if (info.isAllColumns() || info.isDistinct()) {
            throw new SemanticException("The specified syntax for UDAF invocation is invalid.");
        }
        TypeInfo[] parameters = info.getParameters();

        AbstractUDAFRegionEvaluator eval = (AbstractUDAFRegionEvaluator) getEvaluator(parameters);

        eval.setIsAllColumns(info.isAllColumns());
        // FIXME: Commented below because do not work in Spark
        // eval.setWindowing(info.isWindowing());
        eval.setIsDistinct(info.isDistinct());

        return eval;
    }

    public abstract static class AbstractUDAFRegionEvaluator extends GenericUDAFEvaluator {

        class RegionAggregationBuffer extends AbstractAggregationBuffer {
            ADQLRangeSet rs = null;
        }

        protected StructObjectInspector inputOI;

        protected boolean isAllColumns;
        protected boolean isDistinct;
        protected boolean isWindowing;

        public void setIsAllColumns(boolean isAllColumns) {
            this.isAllColumns = isAllColumns;
        }

        public void setIsDistinct(boolean isDistinct) {
            this.isDistinct = isDistinct;
        }

        public void setWindowing(boolean isWindowing) {
            this.isWindowing = isWindowing;
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (parameters.length != 1) {
                throw new UDFArgumentLengthException("This function takes only one argument: region");
            }

            if (!ObjectInspectorUtils.compareTypes(parameters[0], ADQLGeometry.OI)) {
                throw new UDFArgumentTypeException(0, "The argument has to be of ADQL geometry type.");
            }

            inputOI = (StructObjectInspector) parameters[0];

            return ADQLGeometry.OI;
        }

        @Override
        public AbstractAggregationBuffer getNewAggregationBuffer() {
            return new RegionAggregationBuffer();
        }

        @Override
        public void reset(AggregationBuffer agg) {
            ((RegionAggregationBuffer) agg).rs = null;
        }

        protected abstract void doMerge(RegionAggregationBuffer buff, ADQLRegion region);

        @Override
        public void iterate(AggregationBuffer buff, Object[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentLengthException(
                        String.format("A single parameter was expected, got %d instead.", parameters.length));
            }

            if (parameters[0] == null) {
                return;
            }

            RegionAggregationBuffer agg = (RegionAggregationBuffer) buff;
            ADQLRegion region = (ADQLRegion) ADQLGeometry.fromBlob(parameters[0], inputOI);

            doMerge(agg, region);
        }

        @Override
        public void merge(AggregationBuffer buff, Object partial) {
            if (partial == null) {
                return;
            }

            RegionAggregationBuffer agg = (RegionAggregationBuffer) buff;
            ADQLRegion region = (ADQLRegion) ADQLGeometry.fromBlob(partial, inputOI);

            doMerge(agg, region);
        }

        @Override
        public Object terminatePartial(AggregationBuffer buff) throws HiveException {
            RegionAggregationBuffer agg = (RegionAggregationBuffer) buff;

            if (agg.rs == null) {
                return null;
            } else {
                return terminate(buff);
            }
        }

        @Override
        public Object terminate(AggregationBuffer buff) throws HiveException {
            RegionAggregationBuffer agg = (RegionAggregationBuffer) buff;

            if (agg.rs == null) {
                return new ADQLRegion(new ADQLRangeSet()).serialize();
            } else {
                return new ADQLRegion(agg.rs).serialize();
            }
        }
    }
}