package es.pic.hadoop.udf.adql;

import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestIntersection extends AbstractTestUDAFRegion {
    public TestIntersection() {
        udaf = new UDAFIntersection();

        ADQLRangeSet rs;
        Object region1;
        Object region2;
        Object region3;
        Object region4;
        Object region5;

        rs = new ADQLRangeSet();
        rs.addPixelRange(29, 10, 90);
        region1 = new ADQLRegion(rs).serialize();

        rs = new ADQLRangeSet();
        rs.addPixelRange(29, 20, 40);
        region2 = new ADQLRegion(rs).serialize();

        rs = new ADQLRangeSet();
        rs.addPixelRange(29, 30, 80);
        region3 = new ADQLRegion(rs).serialize();

        rs = new ADQLRangeSet();
        rs.addPixelRange(29, 20, 60);
        region4 = new ADQLRegion(rs).serialize();

        rs = new ADQLRangeSet();
        rs.addPixelRange(29, 10, 70);
        region5 = new ADQLRegion(rs).serialize();

        inputs = new Object[] {
                region1, region2, region3, region4, region5
        };

        rs = new ADQLRangeSet();
        rs.addPixelRange(29, 30, 40);
        output = new ADQLRegion(rs).serialize();
    }
}
