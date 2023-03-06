package es.pic.hadoop.udf.array;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestArrayMin extends AbstractTestUDAFArray {
    public TestArrayMin() {
        udaf = new UDAFArrayMin();
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class ByteEvaluator extends AbstractEvaluator {
        public ByteEvaluator() {
            inputElementOI = outputElementOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new ByteWritable((byte) 3)
                    }, new Object[] {
                            new ByteWritable((byte) 2), null
                    }, new Object[] {
                            new ByteWritable((byte) 1), new ByteWritable((byte) 1)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 3]", "[2, 3]", "[1, 1]", "[1, 1]"
            };
            mergedOutput = "[1, 1]";
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class ShortEvaluator extends AbstractEvaluator {
        public ShortEvaluator() {
            inputElementOI = outputElementOI = PrimitiveObjectInspectorFactory.writableShortObjectInspector;
            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new ShortWritable((short) 3)
                    }, new Object[] {
                            new ShortWritable((short) 2), null
                    }, new Object[] {
                            new ShortWritable((short) 1), new ShortWritable((short) 1)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 3]", "[2, 3]", "[1, 1]", "[1, 1]"
            };
            mergedOutput = "[1, 1]";
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class IntEvaluator extends AbstractEvaluator {
        public IntEvaluator() {
            inputElementOI = outputElementOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new IntWritable(3)
                    }, new Object[] {
                            new IntWritable(2), null
                    }, new Object[] {
                            new IntWritable(1), new IntWritable(1)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 3]", "[2, 3]", "[1, 1]", "[1, 1]"
            };
            mergedOutput = "[1, 1]";
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class LongEvaluator extends AbstractEvaluator {
        public LongEvaluator() {
            inputElementOI = outputElementOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new LongWritable(3)
                    }, new Object[] {
                            new LongWritable(2), null
                    }, new Object[] {
                            new LongWritable(1), new LongWritable(1)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 3]", "[2, 3]", "[1, 1]", "[1, 1]"
            };
            mergedOutput = "[1, 1]";
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class FloatEvaluator extends AbstractEvaluator {
        public FloatEvaluator() {
            inputElementOI = outputElementOI = PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new FloatWritable((float) 0.75)
                    }, new Object[] {
                            new FloatWritable((float) 0.50), null
                    }, new Object[] {
                            new FloatWritable((float) 0.25), new FloatWritable((float) 0.5)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 0.75]", "[0.5, 0.75]", "[0.25, 0.5]", "[0.25, 0.5]"
            };
            mergedOutput = "[0.25, 0.5]";
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class DoubleEvaluator extends AbstractEvaluator {
        public DoubleEvaluator() {
            inputElementOI = outputElementOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new DoubleWritable(0.75)
                    }, new Object[] {
                            new DoubleWritable(0.50), null
                    }, new Object[] {
                            new DoubleWritable(0.25), new DoubleWritable(0.50)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 0.75]", "[0.5, 0.75]", "[0.25, 0.5]", "[0.25, 0.5]"
            };
            mergedOutput = "[0.25, 0.5]";
        }
    }
}
