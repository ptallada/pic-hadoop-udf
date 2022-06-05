package es.pic.hadoop.udf.array;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import es.pic.hadoop.udf.array.AbstractUDAFArrayDispersionResolver.AbstractGenericUDAFArrayDispersionEvaluator.ArrayDispersionAggregationBuffer;
import es.pic.hadoop.udf.array.AbstractUDAFArrayResolver.AbstractGenericUDAFArrayEvaluator.ArrayAggregationBuffer;

public abstract class AbstractTestUDAFArray {
    @SuppressWarnings("deprecation")
    protected org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver udaf;

    @Nested
    class Arguments {
        @Test
        @SuppressWarnings("deprecation")
        void isAllColumns() {
            ObjectInspector[] params = new ObjectInspector[0];
            // SimpleGenericUDAFParameterInfo(ObjectInspector[] params, boolean isWindowing, boolean distinct,
            // boolean allColumns)
            GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, true);
            assertThrows(SemanticException.class, () -> udaf.getEvaluator(info));
        }

        @Test
        @SuppressWarnings("deprecation")
        void isDistinct() {
            ObjectInspector[] params = new ObjectInspector[0];
            // SimpleGenericUDAFParameterInfo(ObjectInspector[] params, boolean isWindowing, boolean distinct,
            // boolean allColumns)
            GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, true, false);
            assertThrows(SemanticException.class, () -> udaf.getEvaluator(info));
        }

        @Test
        @SuppressWarnings("deprecation")
        void emptyArguments() {
            ObjectInspector[] params = new ObjectInspector[0];
            GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
            assertThrows(UDFArgumentLengthException.class, () -> udaf.getEvaluator(info));
        }

        @Test
        @SuppressWarnings("deprecation")
        void wrongArgument() {
            ObjectInspector[] params = new ObjectInspector[] {
                    PrimitiveObjectInspectorFactory.writableVoidObjectInspector
            };
            GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
            assertThrows(UDFArgumentTypeException.class, () -> udaf.getEvaluator(info));
        }

        @Test
        @SuppressWarnings("deprecation")
        void wrongListArgument() {
            ObjectInspector[] params = new ObjectInspector[] {
                    ObjectInspectorFactory
                            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableVoidObjectInspector)
            };
            GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
            assertThrows(UDFArgumentTypeException.class, () -> udaf.getEvaluator(info));
        }

        @Test
        @SuppressWarnings("deprecation")
        void wrongComplexListArgument() {
            ObjectInspector[] params = new ObjectInspector[] {
                    ObjectInspectorFactory
                            .getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(
                                    PrimitiveObjectInspectorFactory.writableVoidObjectInspector))
            };
            GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, false);
            assertThrows(UDFArgumentTypeException.class, () -> udaf.getEvaluator(info));
        }
    }

    abstract class AbstractEvaluator {
        protected PrimitiveObjectInspector inputElementOI;
        protected ListObjectInspector inputOI;

        protected PrimitiveObjectInspector outputElementOI;
        protected ListObjectInspector outputOI;

        protected Object[] inputs;
        protected String[] outputs;
        protected String mergedOutput;

        @BeforeAll
        void setupOI() throws Exception {
            inputOI = ObjectInspectorFactory.getStandardListObjectInspector(inputElementOI);
            outputOI = ObjectInspectorFactory.getStandardListObjectInspector(outputElementOI);
        }

        @SuppressWarnings("deprecation")
        GenericUDAFEvaluator getEvaluator() throws SemanticException {
            ObjectInspector[] params = new ObjectInspector[] {
                    ObjectInspectorFactory.getStandardListObjectInspector(inputElementOI)
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
                    inputOI
            });
            assertEquals(outputOI, returnOI);

            partial1OI = evalPartial1.init(GenericUDAFEvaluator.Mode.PARTIAL1, new ObjectInspector[] {
                    inputOI
            });

            partial2OI = evalPartial2.init(GenericUDAFEvaluator.Mode.PARTIAL2, new ObjectInspector[] {
                    partial1OI
            });
            assertEquals(partial1OI, partial2OI);

            returnOI = evalFinal.init(GenericUDAFEvaluator.Mode.FINAL, new ObjectInspector[] {
                    partial2OI
            });
            assertEquals(outputOI, returnOI);
        }

        @Test
        void emptyArguments() throws Exception {
            GenericUDAFEvaluator eval = getEvaluator();
            assertThrows(UDFArgumentLengthException.class,
                    () -> eval.init(GenericUDAFEvaluator.Mode.COMPLETE, new ObjectInspector[] {}));
        }

        @Test
        void wrongListArgument() throws Exception {
            GenericUDAFEvaluator eval = getEvaluator();
            PrimitiveObjectInspector wrongElementOI = PrimitiveObjectInspectorFactory.writableVoidObjectInspector;
            ListObjectInspector wrongOI = ObjectInspectorFactory.getStandardListObjectInspector(wrongElementOI);

            assertThrows(UDFArgumentTypeException.class,
                    () -> eval.init(GenericUDAFEvaluator.Mode.COMPLETE, new ObjectInspector[] {
                            wrongOI
                    }));
        }

        @Test
        void testMultipleArguments() throws Exception {
            GenericUDAFEvaluator eval = getEvaluator();
            eval.init(GenericUDAFEvaluator.Mode.COMPLETE, new ObjectInspector[] {
                    inputOI
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
        void testLengthMismatch() throws Exception {
            GenericUDAFEvaluator eval = getEvaluator();
            eval.init(GenericUDAFEvaluator.Mode.COMPLETE, new ObjectInspector[] {
                    inputOI
            });

            AbstractAggregationBuffer agg = (AbstractAggregationBuffer) eval.getNewAggregationBuffer();
            Object[] inputs = new Object[] {
                    new Object[] {
                            null
                    }, new Object[] {
                            null, null
                    }
            };
            eval.iterate(agg, new Object[] {
                    inputs[0]
            });
            assertThrows(UDFArgumentException.class, () -> eval.iterate(agg, new Object[] {
                    inputs[1]
            }));
        }

        @Test
        void testIterate() throws Exception {
            GenericUDAFEvaluator eval = getEvaluator();
            eval.init(GenericUDAFEvaluator.Mode.COMPLETE, new ObjectInspector[] {
                    inputOI
            });

            @SuppressWarnings("deprecation")
            AggregationBuffer agg = eval.getNewAggregationBuffer();

            // Test null value
            eval.iterate(agg, new Object[] {
                    null
            });

            for (int i = 0; i < inputs.length; i++) {
                eval.iterate(agg, new Object[] {
                        inputs[i]
                });

                assertEquals(eval.terminate(agg).toString(), outputs[i]);
            }

            eval.reset(agg);
        }

        @Test
        void testMerge() throws Exception {
            GenericUDAFEvaluator evalPartial1 = getEvaluator();
            GenericUDAFEvaluator evalPartial2 = getEvaluator();
            GenericUDAFEvaluator evalFinal = getEvaluator();

            Object partial_null;
            Object partial_zero;
            Object partial1;
            Object partial2;
            ObjectInspector partialOI;

            partialOI = evalPartial1.init(GenericUDAFEvaluator.Mode.PARTIAL1, new ObjectInspector[] {
                    inputOI
            });
            evalPartial2.init(GenericUDAFEvaluator.Mode.PARTIAL2, new ObjectInspector[] {
                    partialOI
            });
            evalFinal.init(GenericUDAFEvaluator.Mode.FINAL, new ObjectInspector[] {
                    partialOI
            });

            @SuppressWarnings("deprecation")
            AggregationBuffer agg1 = evalPartial1.getNewAggregationBuffer();
            @SuppressWarnings("deprecation")
            AggregationBuffer agg2 = evalPartial2.getNewAggregationBuffer();
            @SuppressWarnings("deprecation")
            AggregationBuffer aggF = evalFinal.getNewAggregationBuffer();

            // Test null reference
            evalPartial2.merge(agg2, null); // Must not throw any exception

            // Test null counters
            partial_null = evalPartial1.terminatePartial(agg1);
            evalPartial2.merge(agg2, partial_null);
            assertEquals(evalPartial2.terminate(agg2), null);

            // Test 0 counters
            evalPartial1.iterate(agg1, new Object[] {
                    new Object[] {
                            null, null
                    }
            });
            partial_zero = evalPartial1.terminatePartial(agg1);
            evalPartial2.merge(agg2, partial_zero);
            assertEquals(evalPartial2.terminate(agg2).toString(), "[null, null]");

            // Normal case
            for (int i = 0; i < inputs.length; i++) {
                evalPartial1.iterate(agg1, new Object[] {
                        inputs[i]
                });
            }
            partial1 = evalPartial1.terminatePartial(agg1);
            evalPartial2.merge(agg2, partial1);
            evalPartial2.merge(agg2, partial_zero);
            partial2 = evalPartial2.terminatePartial(agg2);

            if (partial1 instanceof ArrayAggregationBuffer) {
                @SuppressWarnings("rawtypes")
                ArrayAggregationBuffer p1 = (ArrayAggregationBuffer) partial1;
                @SuppressWarnings("rawtypes")
                ArrayAggregationBuffer p2 = (ArrayAggregationBuffer) partial2;
                assertEquals(p1.array, p2.array);
            } else if (partial1 instanceof ArrayDispersionAggregationBuffer) {
                assertEquals(((ArrayDispersionAggregationBuffer) partial1).count,
                        ((ArrayDispersionAggregationBuffer) partial2).count);
                assertEquals(((ArrayDispersionAggregationBuffer) partial1).sum,
                        ((ArrayDispersionAggregationBuffer) partial2).sum);
                assertEquals(((ArrayDispersionAggregationBuffer) partial1).var,
                        ((ArrayDispersionAggregationBuffer) partial2).var);
            }
            evalFinal.merge(aggF, partial1);
            evalFinal.merge(aggF, partial2);

            assertEquals(evalFinal.terminate(aggF).toString(), mergedOutput);
        }
    }
}
