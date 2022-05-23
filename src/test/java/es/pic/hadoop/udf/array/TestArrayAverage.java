package es.pic.hadoop.udf.array;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import es.pic.hadoop.udf.array.UDAFArrayAverage.UDAFArrayAverageEvaluator.ArrayAverageAggregationBuffer;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestArrayAverage {

    protected UDAFArrayAverage udaf = new UDAFArrayAverage();

    @Nested
    class Arguments {
        @Test
        void isAllColumns() {
            ObjectInspector[] params = new ObjectInspector[0];
            // SimpleGenericUDAFParameterInfo(ObjectInspector[] params, boolean isWindowing, boolean distinct, boolean allColumns)
            GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, false, true);
            assertThrows(SemanticException.class, () -> udaf.getEvaluator(info));
        }

        @Test
        void isDistinct() {
            ObjectInspector[] params = new ObjectInspector[0];
            // SimpleGenericUDAFParameterInfo(ObjectInspector[] params, boolean isWindowing, boolean distinct, boolean allColumns)
            GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(params, false, true, false);
            assertThrows(SemanticException.class, () -> udaf.getEvaluator(info));
        }

        @Test
        void emptyArguments() {
            TypeInfo[] parameters = new TypeInfo[0];
            assertThrows(UDFArgumentLengthException.class, () -> udaf.getEvaluator(parameters));
        }

        @Test
        void wrongArgument() {
            TypeInfo[] parameters = new TypeInfo[] {
                    TypeInfoFactory.unknownTypeInfo
            };
            assertThrows(UDFArgumentTypeException.class, () -> udaf.getEvaluator(parameters));
        }

        @Test
        void wrongListArgument() {
            TypeInfo[] parameters = new TypeInfo[] {
                    TypeInfoFactory.getListTypeInfo(TypeInfoFactory.unknownTypeInfo)
            };
            assertThrows(UDFArgumentTypeException.class, () -> udaf.getEvaluator(parameters));
        }
    }

    abstract class AbstractEvaluator {
        protected PrimitiveTypeInfo inputPrimitiveType;

        protected PrimitiveObjectInspector inputElementOI;
        protected ListObjectInspector inputOI;

        protected PrimitiveObjectInspector outputElementOI;
        protected ListObjectInspector outputOI;

        protected Object[] inputs;
        protected String[] outputs;

        GenericUDAFEvaluator createEvaluator() throws Exception {
            inputOI = ObjectInspectorFactory.getStandardListObjectInspector(inputElementOI);
            outputOI = ObjectInspectorFactory.getStandardListObjectInspector(outputElementOI);

            TypeInfo[] parameters = new TypeInfo[] {
                    TypeInfoFactory.getListTypeInfo(inputPrimitiveType)
            };

            return udaf.getEvaluator(parameters);
        }

        @Test
        void wrongListArgument() throws Exception {
            GenericUDAFEvaluator eval = createEvaluator();

            PrimitiveObjectInspector wrongElementOI = PrimitiveObjectInspectorFactory.writableVoidObjectInspector;
            ListObjectInspector wrongOI = ObjectInspectorFactory.getStandardListObjectInspector(wrongElementOI);

            assertThrows(UDFArgumentTypeException.class,
                    () -> eval.init(GenericUDAFEvaluator.Mode.COMPLETE, new ObjectInspector[] {
                            wrongOI
                    }));
        }

        @Test
        GenericUDAFEvaluator initEvaluator() throws Exception {
            GenericUDAFEvaluator eval = createEvaluator();
            ObjectInspector returnOI = eval.init(GenericUDAFEvaluator.Mode.COMPLETE, new ObjectInspector[] {
                    inputOI
            });

            assertEquals(outputOI, returnOI);

            return eval;
        }

        @Test
        void testLengthMismatch() throws Exception {
            GenericUDAFEvaluator eval = initEvaluator();
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
            GenericUDAFEvaluator eval = initEvaluator();
            ArrayAverageAggregationBuffer agg = (ArrayAverageAggregationBuffer) eval.getNewAggregationBuffer();

            for (int i = 0; i < inputs.length; i++) {
                eval.iterate(agg, new Object[] {
                        inputs[i]
                });

                assertEquals(eval.terminate(agg).toString(), outputs[i]);
            }
        }

        /*@Test
        void testMerge() throws Exception {
            GenericUDAFEvaluator eval = initEvaluator();
        
            ArrayAverageAggregationBuffer agg1 = (ArrayAverageAggregationBuffer) eval.getNewAggregationBuffer();
            ArrayAverageAggregationBuffer agg2 = (ArrayAverageAggregationBuffer) eval.getNewAggregationBuffer();
        
            for (int i = 0; i < inputs.length; i++) {
                eval.iterate(agg1, new Object[] {
                        inputs[i]
                });
            }
        
            Object partial = eval.terminatePartial(agg1);
        
            eval.merge(agg2, null); // Must not throw any exception
            eval.merge(agg2, partial);
        
            assertEquals(eval.terminate(agg1).toString(), eval.terminate(agg2).toString());
            assertEquals(eval.terminate(agg2).toString(), outputs[outputs.length - 1]);
        }*/
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class ByteEvaluator extends AbstractEvaluator {

        public ByteEvaluator() {
            inputPrimitiveType = TypeInfoFactory.byteTypeInfo;

            inputElementOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
            outputElementOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new ByteWritable((byte) 1)
                    }, new Object[] {
                            new ByteWritable((byte) 2), null
                    }, new Object[] {
                            new ByteWritable((byte) 3), new ByteWritable((byte) 3)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 1.0]", "[2.0, 1.0]", "[2.5, 2.0]", "[2.5, 2.0]"
            };
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class ShortEvaluator extends AbstractEvaluator {
        public ShortEvaluator() {
            inputPrimitiveType = TypeInfoFactory.shortTypeInfo;

            inputElementOI = PrimitiveObjectInspectorFactory.writableShortObjectInspector;
            outputElementOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new ShortWritable((short) 1)
                    }, new Object[] {
                            new ShortWritable((short) 2), null
                    }, new Object[] {
                            new ShortWritable((short) 3), new ShortWritable((short) 3)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 1.0]", "[2.0, 1.0]", "[2.5, 2.0]", "[2.5, 2.0]"
            };
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class IntEvaluator extends AbstractEvaluator {
        public IntEvaluator() {
            inputPrimitiveType = TypeInfoFactory.intTypeInfo;

            inputElementOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
            outputElementOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new IntWritable(1)
                    }, new Object[] {
                            new IntWritable(2), null
                    }, new Object[] {
                            new IntWritable(3), new IntWritable(3)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 1.0]", "[2.0, 1.0]", "[2.5, 2.0]", "[2.5, 2.0]"
            };
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class LongEvaluator extends AbstractEvaluator {
        public LongEvaluator() {
            inputPrimitiveType = TypeInfoFactory.longTypeInfo;

            inputElementOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            outputElementOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new LongWritable(1)
                    }, new Object[] {
                            new LongWritable(2), null
                    }, new Object[] {
                            new LongWritable(3), new LongWritable(3)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 1.0]", "[2.0, 1.0]", "[2.5, 2.0]", "[2.5, 2.0]"
            };
        }

    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class FloatEvaluator extends AbstractEvaluator {
        public FloatEvaluator() {
            inputPrimitiveType = TypeInfoFactory.floatTypeInfo;

            inputElementOI = PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
            outputElementOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new FloatWritable((float) 1.5)
                    }, new Object[] {
                            new FloatWritable((float) 1.0), null
                    }, new Object[] {
                            new FloatWritable((float) 0.5), new FloatWritable((float) 0.5)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 1.5]", "[1.0, 1.5]", "[0.75, 1.0]", "[0.75, 1.0]"
            };
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class DoubleEvaluator extends AbstractEvaluator {
        public DoubleEvaluator() {
            inputPrimitiveType = TypeInfoFactory.doubleTypeInfo;

            inputElementOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            outputElementOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new DoubleWritable(1.5)
                    }, new Object[] {
                            new DoubleWritable(1.0), null
                    }, new Object[] {
                            new DoubleWritable(0.5), new DoubleWritable(0.5)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 1.5]", "[1.0, 1.5]", "[0.75, 1.0]", "[0.75, 1.0]"
            };
        }
    }
}
