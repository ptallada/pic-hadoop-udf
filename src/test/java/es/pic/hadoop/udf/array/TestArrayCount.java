package es.pic.hadoop.udf.array;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestArrayCount extends AbstractTestUDAFArray {
    public TestArrayCount() {
        udaf = new UDAFArrayCount();
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class ByteEvaluator extends AbstractEvaluator {
        public ByteEvaluator() {
            inputElementOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
            outputElementOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
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
                    "[null, null]", "[null, 1]", "[1, 1]", "[2, 2]", "[2, 2]"
            };
            mergedOutput = "[4, 4]";
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class ShortEvaluator extends AbstractEvaluator {
        public ShortEvaluator() {
            inputElementOI = PrimitiveObjectInspectorFactory.writableShortObjectInspector;
            outputElementOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
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
                    "[null, null]", "[null, 1]", "[1, 1]", "[2, 2]", "[2, 2]"
            };
            mergedOutput = "[4, 4]";
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class IntEvaluator extends AbstractEvaluator {
        public IntEvaluator() {
            inputElementOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
            outputElementOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
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
                    "[null, null]", "[null, 1]", "[1, 1]", "[2, 2]", "[2, 2]"
            };
            mergedOutput = "[4, 4]";
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class LongEvaluator extends AbstractEvaluator {
        public LongEvaluator() {
            inputElementOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            outputElementOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
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
                    "[null, null]", "[null, 1]", "[1, 1]", "[2, 2]", "[2, 2]"
            };
            mergedOutput = "[4, 4]";
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class FloatEvaluator extends AbstractEvaluator {
        public FloatEvaluator() {
            inputElementOI = PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
            outputElementOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new FloatWritable((float) 0.25)
                    }, new Object[] {
                            new FloatWritable((float) 0.50), null
                    }, new Object[] {
                            new FloatWritable((float) 0.75), new FloatWritable((float) 0.75)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 1]", "[1, 1]", "[2, 2]", "[2, 2]"
            };
            mergedOutput = "[4, 4]";
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class DoubleEvaluator extends AbstractEvaluator {
        public DoubleEvaluator() {
            inputElementOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            outputElementOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            inputs = new Object[] {
                    new Object[] {
                            null, null,
                    }, new Object[] {
                            null, new DoubleWritable(0.25)
                    }, new Object[] {
                            new DoubleWritable(0.50), null
                    }, new Object[] {
                            new DoubleWritable(0.75), new DoubleWritable(0.75)
                    }, new Object[] {
                            null, null
                    },
            };
            outputs = new String[] {
                    "[null, null]", "[null, 1]", "[1, 1]", "[2, 2]", "[2, 2]"
            };
            mergedOutput = "[4, 4]";
        }
    }
}
