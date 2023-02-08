package es.pic.hadoop.udf.adql;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.jupiter.api.TestInstance;

import healpix.essentials.Moc;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestUnion extends AbstractTestUDAFRegion {
    public TestUnion() throws HiveException {
        udaf = new UDAFUnion();

        Moc moc;
        Object region1;
        Object region2;
        Object region3;
        Object region4;
        Object region5;

        moc = new Moc();
        moc.addPixelRange(29, 10, 30);
        region1 = new ADQLRegion(moc).serialize();

        moc = new Moc();
        moc.addPixelRange(29, 40, 60);
        region2 = new ADQLRegion(moc).serialize();

        moc = new Moc();
        moc.addPixelRange(29, 10, 20);
        region3 = new ADQLRegion(moc).serialize();

        moc = new Moc();
        moc.addPixelRange(29, 20, 40);
        region4 = new ADQLRegion(moc).serialize();

        moc = new Moc();
        moc.addPixelRange(29, 50, 70);
        region5 = new ADQLRegion(moc).serialize();

        inputs = new Object[] {
                region1, region2, region3, region4, region5
        };

        moc = new Moc();
        moc.addPixelRange(29, 10, 70);
        output = new ADQLRegion(moc).serialize();
    }
}
