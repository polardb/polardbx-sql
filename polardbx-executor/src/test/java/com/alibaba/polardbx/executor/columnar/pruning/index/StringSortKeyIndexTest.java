package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

public class StringSortKeyIndexTest {
    private final String[] data = new String[] {"aaa", "bbb", "ddd", "ggg", "ggg", "jjj", "xxx", "zzz"};

    private final StringSortKeyIndex sortKeyIndex = StringSortKeyIndex.build(1, data, DataTypes.StringType);

    @Test
    public void testEqual() {
        RoaringBitmap rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("eee", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(1));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("ccc", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("ggg", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(RoaringBitmap.bitmapOf(1, 2)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("hhh", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(RoaringBitmap.bitmapOf(2)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("www", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("xxx", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(RoaringBitmap.bitmapOf(3)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("zzz", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(RoaringBitmap.bitmapOf(3)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("zzza", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("aaa", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(RoaringBitmap.bitmapOf(0)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("aa", rs);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 0);
    }

    @Test
    public void testRange() {
        // test start obj less than the lowest value
        RoaringBitmap rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("a", "aa", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aa", "aa", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("a", "aaa", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("a", "b", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("a", "bb", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("a", "bbb", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bbb", "bbb", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bb", "bbb", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bb", "ddd", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(0) && rs.contains(1));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bbb", "eee", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(0) && rs.contains(1));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bba", "ggg", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 3 && rs.contains(0) && rs.contains(1) && rs.contains(2));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bba", "xxxa", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bba", "xxx", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bba", "xxxy", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aaa", "bb", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aaa", "bbb", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aaa", "bbbd", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aaa", "gga", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(0) && rs.contains(1));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aaa", "ggg", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 3 && rs.contains(0) && rs.contains(1) && rs.contains(2));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("ddd", "ggg", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(1) && rs.contains(2));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("ddd", "xxx", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 3 && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("xxx", "zzz", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("zzza", "zzzb", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("zzzzzzzz", "zzzzzzzz", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bbb", "zzzz", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(null, "zzz", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == sortKeyIndex.rgNum());

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("zzz", null, rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(3));
    }

    @Test
    public void testInvalidRange() {
        // test start obj less than the lowest value
        RoaringBitmap rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aa", "a", rs);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);
    }

    @Test
    public void testSize() {
        long expectedSize = 0;
        for (String datum : data) {
            expectedSize += datum.getBytes().length;
        }
        Assert.assertEquals(expectedSize, sortKeyIndex.getSizeInBytes());
    }
}
