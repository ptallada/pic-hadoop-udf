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
import org.apache.hadoop.io.FloatWritable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestAng2Vec {

    UDFAng2Vec udf = new UDFAng2Vec();

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
        };

        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 0)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 1)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 4)));
    }

    @Test
    void nullValues() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(new FloatWritable((float) 50)),
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new FloatWritable((float) 50)), new DeferredJavaObject(null),
        }));
    }

    @Test
    void validValues() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("[0.6040227735550536, 0.7198463103929541, 0.3420201433256688]",
                udf.evaluate(new DeferredJavaObject[] {
                        new DeferredJavaObject(new FloatWritable((float) 50)),
                        new DeferredJavaObject(new FloatWritable((float) 20)),
                        new DeferredJavaObject(new BooleanWritable(true)),
                }).toString());
        assertEquals("[0.4698689466656567, 0.09524715232087738, 0.8775825618903728]",
                udf.evaluate(new DeferredJavaObject[] {
                        new DeferredJavaObject(new FloatWritable((float) 0.5)),
                        new DeferredJavaObject(new FloatWritable((float) 0.2)),
                        new DeferredJavaObject(new BooleanWritable(false)),
                }).toString());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}
