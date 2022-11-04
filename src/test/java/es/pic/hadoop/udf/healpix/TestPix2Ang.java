package es.pic.hadoop.udf.healpix;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPix2Ang {

    UDFPix2Ang udf = new UDFPix2Ang();

    ObjectInspector outputOI = ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

    @Test
    void emptyArguments() {
        ObjectInspector[] params = new ObjectInspector[0];
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(params));
    }

    @Test
    void wrongNumberOfArguments() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 1)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 5)));
    }

    @Test
    void nullValues() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableByteObjectInspector,
                PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(new LongWritable(50))
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 10)), new DeferredJavaObject(null)
        }));
    }

    @Test
    void invalidValues() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableByteObjectInspector,
                PrimitiveObjectInspectorFactory.writableLongObjectInspector,
                PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertThrows(HiveException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 50)), new DeferredJavaObject(new LongWritable(50)),
                new DeferredJavaObject(new BooleanWritable(true))
        }));
        assertThrows(HiveException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 10)), new DeferredJavaObject(new LongWritable(-50)),
                new DeferredJavaObject(new BooleanWritable(true))
        }));
    }

    @Test
    void validValues() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableByteObjectInspector,
                PrimitiveObjectInspectorFactory.writableLongObjectInspector,
                PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
                PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("[50.00976562499999, 19.98638727226823]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 10)), new DeferredJavaObject(new LongWritable(108023)),
                new DeferredJavaObject(new BooleanWritable(true)), new DeferredJavaObject(new BooleanWritable(true)),
        }).toString());
        assertEquals("[50.00976562499999, 19.98638727226823]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 10)), new DeferredJavaObject(new LongWritable(4139577)),
                new DeferredJavaObject(new BooleanWritable(false)), new DeferredJavaObject(new BooleanWritable(true)),
        }).toString());
        assertEquals("[0.5003639274847129, 0.20109228338195537]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 10)), new DeferredJavaObject(new LongWritable(777732)),
                new DeferredJavaObject(new BooleanWritable(true)), new DeferredJavaObject(new BooleanWritable(false)),
        }).toString());
        assertEquals("[0.5003639274847129, 0.20109228338195537]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 10)), new DeferredJavaObject(new LongWritable(770119)),
                new DeferredJavaObject(new BooleanWritable(false)), new DeferredJavaObject(new BooleanWritable(false)),
        }).toString());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}
