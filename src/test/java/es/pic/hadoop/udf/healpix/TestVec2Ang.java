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
public class TestVec2Ang {

    UDFVec2Ang udf = new UDFVec2Ang();

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
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 2)));
        assertThrows(UDFArgumentLengthException.class, () -> udf.initialize(Arrays.copyOfRange(params, 0, 5)));
    }

    @Test
    void nullValues() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(null), new DeferredJavaObject(new FloatWritable((float) 0.5)),
                new DeferredJavaObject(new FloatWritable((float) 0.5))
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new FloatWritable((float) 0.5)), new DeferredJavaObject(null),
                new DeferredJavaObject(new FloatWritable((float) 0.5))
        }));
        assertNull(udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new FloatWritable((float) 0.5)),
                new DeferredJavaObject(new FloatWritable((float) 0.5)), new DeferredJavaObject(null)
        }));
    }

    @Test
    void validValues() throws HiveException {
        ObjectInspector[] params = new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
                PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        };

        assertEquals(udf.initialize(params), outputOI);

        assertEquals("[49.99999863700896, 20.000000262397222]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new FloatWritable((float) 0.604022773)),
                new DeferredJavaObject(new FloatWritable((float) 0.719846310)),
                new DeferredJavaObject(new FloatWritable((float) 0.342020143)),
                new DeferredJavaObject(new BooleanWritable(true)),
        }).toString());
        assertEquals("[0.5000000149250752, 0.19999999245180738]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new FloatWritable((float) 0.469868946)),
                new DeferredJavaObject(new FloatWritable((float) 0.095247152)),
                new DeferredJavaObject(new FloatWritable((float) 0.877582561)),
                new DeferredJavaObject(new BooleanWritable(false)),
        }).toString());
        assertEquals("[315.0, 35.264389682754654]", udf.evaluate(new DeferredJavaObject[] {
                new DeferredJavaObject(new FloatWritable((float) 0.5)),
                new DeferredJavaObject(new FloatWritable((float) -0.5)),
                new DeferredJavaObject(new FloatWritable((float) 0.5)),
                new DeferredJavaObject(new BooleanWritable(true)),
        }).toString());
    }

    @Test
    void displayString() {
        assertDoesNotThrow(() -> udf.getDisplayString(new String[] {}));
    }
}
