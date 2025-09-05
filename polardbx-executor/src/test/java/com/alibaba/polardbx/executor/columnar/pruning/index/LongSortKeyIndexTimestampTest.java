package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

/**
 * @author fangwu
 */
public class LongSortKeyIndexTimestampTest {
    /**
     * 2024-01-15 14:02:11.657 28610161799726696
     * 2025-01-15 14:02:11.657 29140697634965096
     * 2025-01-16 14:02:11.657 29142147186427496
     * 2025-01-17 14:02:11.657 29143596737889896
     */
    private final long[] data =
        new long[] {28610161799726696L, 29140697634965096L, 29142147186427496L, 29143596737889896L};
    private final LongSortKeyIndex sortKeyIndex = LongSortKeyIndex.build(1, data, DataTypes.TimestampType);

    @Test
    public void testEqual() {
        RoaringBitmap rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("2024-01-15 14:02:11.657", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("2025-01-16 14:02:11.657", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(1));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("2023-01-15 14:02:11.657", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertEquals(0, rs.getCardinality());

    }

    @Test
    public void testRange() {
        // test start obj less than the lowest value
        RoaringBitmap rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("2024-01-15 14:02:11.657", "2024-01-13 14:02:11.657", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("2024-01-15 14:02:11.657", "2025-01-17 14:02:11.657", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(0) && rs.contains(1));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("2024-01-15 14:02:11.657", null, rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(0) && rs.contains(1));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("2024-01-15 14:02:11.657", "2025-01-15 15:02:11.657", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));
    }
}
