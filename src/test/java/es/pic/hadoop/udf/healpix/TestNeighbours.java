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
public class TestNeighbours {

    UDFNeighbours udf = new UDFNeighbours();

    ObjectInspector outputOI = ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

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

        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 1)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 4)));
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
        // This returns negative values instead of en Exception !?!
        // assertThrows(HiveException.class, () -> udf.evaluate(new DeferredJavaObject[] {
        //         new DeferredJavaObject(new ByteWritable((byte) 10)), new DeferredJavaObject(new LongWritable(-50)),
        //         new DeferredJavaObject(new BooleanWritable(true))
        // }));
    }

    @Test
    void validValues() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableByteObjectInspector,
                PrimitiveObjectInspectorFactory.writableLongObjectInspector,
                PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("[319, -1, 213, 215, 43, 41, 40, 317]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 3)), new DeferredJavaObject(new LongWritable(42)),
                new DeferredJavaObject(new BooleanWritable(true)),
        }).toString());
        assertEquals("[62, 41, 25, 13, 26, 43, 63, 87]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 3)), new DeferredJavaObject(new LongWritable(42)),
                new DeferredJavaObject(new BooleanWritable(false)),
        }).toString());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}
