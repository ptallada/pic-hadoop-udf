package es.pic.hadoop.udf.adql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import es.pic.hadoop.udf.adql.AbstractUDAFRegionResolver.AbstractUDAFRegionEvaluator.RegionAggregationBuffer;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractTestUDAFRegion {

    protected AbstractUDAFRegionResolver udaf;

    protected Object[] inputs;
    protected Object output;

    @Test
    void isAllColumns() {
        ObjectInspector[] params = new ObjectInspector[0];
        // SimpleGenericUDAFParameterInfo(ObjectInspector[] params, boolean isWindowing, boolean distinct,
        // boolean allColumns)
        GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, true);
        assertThrows(SemanticException.class, () -> udaf.getEvaluator(info));
    }

    @Test
    void isDistinct() {
        ObjectInspector[] params = new ObjectInspector[0];
        // SimpleGenericUDAFParameterInfo(ObjectInspector[] params, boolean isWindowing, boolean distinct,
        // boolean allColumns)
        GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, true, false);
        assertThrows(SemanticException.class, () -> udaf.getEvaluator(info));
    }

    @Test
    void emptyArguments() {
        ObjectInspector[] params = new ObjectInspector[0];
        GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
        assertThrows(UDFArgumentLengthException.class, () -> udaf.getEvaluator(info));
    }

    GenericUDAFEvaluator getEvaluator() throws SemanticException {
        ObjectInspector[] params = new ObjectInspector[] {
                ADQLGeometry.OI
        };
        GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
        return udaf.getEvaluator(info);
    }

    @Test
    void outputOI() throws Exception {
        GenericUDAFEvaluator evalComplete = getEvaluator();
        GenericUDAFEvaluator evalPartial1 = getEvaluator();
        GenericUDAFEvaluator evalPartial2 = getEvaluator();
        GenericUDAFEvaluator evalFinal = getEvaluator();

        ObjectInspector returnOI;
        ObjectInspector partial1OI;
        ObjectInspector partial2OI;

        returnOI = evalComplete.init(GenericUDAFEvaluator.Mode.COMPLETE, new ObjectInspector[] {
                ADQLGeometry.OI
        });
        assertEquals(ADQLGeometry.OI, returnOI);

        partial1OI = evalPartial1.init(GenericUDAFEvaluator.Mode.PARTIAL1, new ObjectInspector[] {
                ADQLGeometry.OI
        });

        partial2OI = evalPartial2.init(GenericUDAFEvaluator.Mode.PARTIAL2, new ObjectInspector[] {
                partial1OI
        });
        assertEquals(partial1OI, partial2OI);

        returnOI = evalFinal.init(GenericUDAFEvaluator.Mode.FINAL, new ObjectInspector[] {
                partial2OI
        });
        assertEquals(ADQLGeometry.OI, returnOI);
    }

    @Test
    void wrongNumberOfArguments() throws Exception {
        GenericUDAFEvaluator eval = getEvaluator();
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
        };

        assertThrows(UDFArgumentLengthException.class,
                () -> eval.init(GenericUDAFEvaluator.Mode.COMPLETE, Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class,
                () -> eval.init(GenericUDAFEvaluator.Mode.COMPLETE, Arrays.copyOfRange(params, 0, 2)));

    }

    @Test
    void wrongTypeOfArguments() throws Exception {
        GenericUDAFEvaluator eval = getEvaluator();
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
        };

        assertThrows(UDFArgumentTypeException.class, () -> eval.init(GenericUDAFEvaluator.Mode.COMPLETE, params));
    }

    @Test
    void testMultipleArguments() throws Exception {
        GenericUDAFEvaluator eval = getEvaluator();
        eval.init(GenericUDAFEvaluator.Mode.COMPLETE, new ObjectInspector[] {
                ADQLGeometry.OI
        });

        AbstractAggregationBuffer agg = (AbstractAggregationBuffer) eval.getNewAggregationBuffer();
        Object[] inputs = new Object[] {
                new Object[] {
                        null, null
                }, new Object[] {
                        null, null
                }
        };

        assertThrows(UDFArgumentLengthException.class, () -> eval.iterate(agg, inputs));
    }

    @Test
    void testIterate() throws Exception {
        GenericUDAFEvaluator eval = getEvaluator();
        eval.init(GenericUDAFEvaluator.Mode.COMPLETE, new ObjectInspector[] {
                ADQLGeometry.OI
        });

        RegionAggregationBuffer agg = (RegionAggregationBuffer) eval.getNewAggregationBuffer();

        // Test null value
        eval.iterate(agg, new Object[] {
                null
        });

        for (int i = 0; i < inputs.length; i++) {
            eval.iterate(agg, new Object[] {
                    inputs[i]
            });
        }

        assertEquals(eval.terminate(agg), output);

        eval.reset(agg);
    }

    @Test
    void testMerge() throws Exception {
        GenericUDAFEvaluator evalPartial1 = getEvaluator();
        GenericUDAFEvaluator evalPartial2 = getEvaluator();
        GenericUDAFEvaluator evalFinal = getEvaluator();

        Object partial1;
        Object partial2;

        evalPartial1.init(GenericUDAFEvaluator.Mode.PARTIAL1, new ObjectInspector[] {
                ADQLGeometry.OI
        });
        evalPartial2.init(GenericUDAFEvaluator.Mode.PARTIAL2, new ObjectInspector[] {
                ADQLGeometry.OI
        });
        evalFinal.init(GenericUDAFEvaluator.Mode.FINAL, new ObjectInspector[] {
                ADQLGeometry.OI
        });

        RegionAggregationBuffer agg1 = (RegionAggregationBuffer) evalPartial1.getNewAggregationBuffer();
        RegionAggregationBuffer agg2 = (RegionAggregationBuffer) evalPartial2.getNewAggregationBuffer();
        RegionAggregationBuffer aggF = (RegionAggregationBuffer) evalFinal.getNewAggregationBuffer();

        // Test null reference
        evalPartial2.merge(agg2, null); // Must not throw any exception

        // Normal case
        for (int i = 0; i < inputs.length; i++) {
            evalPartial1.iterate(agg1, new Object[] {
                    inputs[i]
            });
        }
        partial1 = evalPartial1.terminatePartial(agg1);
        evalPartial2.merge(agg2, partial1);
        partial2 = evalPartial2.terminatePartial(agg2);

        assertEquals(partial1, partial2);

        evalFinal.merge(aggF, partial1);
        evalFinal.merge(aggF, partial2);

        assertEquals(evalFinal.terminate(aggF), output);
    }
}
