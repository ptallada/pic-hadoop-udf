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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.io.FloatWritable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestVec2Pix {

    UDFVec2Pix udf = new UDFVec2Pix();

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;

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
                PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 1)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 2)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 3)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 6)));
    }

    @Test
    void nullValues() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableByteObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 10)), new DeferredJavaObject(null),
                new DeferredJavaObject(new FloatWritable((float) 0.5)),
                new DeferredJavaObject(new FloatWritable((float) 0.5))
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 10)),
                new DeferredJavaObject(new FloatWritable((float) 0.5)), new DeferredJavaObject(null),
                new DeferredJavaObject(new FloatWritable((float) 0.5))
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 10)),
                new DeferredJavaObject(new FloatWritable((float) 0.5)),
                new DeferredJavaObject(new FloatWritable((float) 0.5)), new DeferredJavaObject(null)
        }));
    }

    @Test
    void invalidValues() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableByteObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertThrows(HiveException.class, () -> udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 50)),
                new DeferredJavaObject(new FloatWritable((float) 0.5)),
                new DeferredJavaObject(new FloatWritable((float) 0.5)),
                new DeferredJavaObject(new FloatWritable((float) 0.5)),
        }));
    }

    @Test
    void validValues() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableByteObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("108023", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 10)),
                new DeferredJavaObject(new FloatWritable((float) 0.6039522774664349)),
                new DeferredJavaObject(new FloatWritable((float) 0.7200114879523387)),
                new DeferredJavaObject(new FloatWritable((float) 0.341796875)),
                new DeferredJavaObject(new BooleanWritable(true)),
        }).toString());
        assertEquals("4139577", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new ByteWritable((byte) 10)),
                new DeferredJavaObject(new FloatWritable((float) 0.6039522774664349)),
                new DeferredJavaObject(new FloatWritable((float) 0.7200114879523387)),
                new DeferredJavaObject(new FloatWritable((float) 0.341796875)),
                new DeferredJavaObject(new BooleanWritable(false)),
        }).toString());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}
