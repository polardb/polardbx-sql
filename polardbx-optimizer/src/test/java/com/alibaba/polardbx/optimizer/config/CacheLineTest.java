package com.alibaba.polardbx.optimizer.config;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.config.table.statistic.TopN;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.sql.Date;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.common.properties.ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME;
import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;

/**
 * @author fangwu
 */
public class CacheLineTest {

    @Test
    public void testExpire() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        int expiredTime = InstConfUtil.getInt(ConnectionParams.STATISTIC_EXPIRE_TIME);
        StatisticManager.CacheLine c =
            new StatisticManager.CacheLine(1, unixTimeStamp() - expiredTime - 1, unixTimeStamp());

        c.setTopN("test_col", new TopN(DataTypes.IntegerType, 1.0));
        Assert.assertTrue(c.hasExpire());

        c.setRowCount(10L);

        Assert.assertTrue(c.hasExpire());

        c.setLastModifyTime(unixTimeStamp() - expiredTime + 100);
        Assert.assertTrue(!c.hasExpire());
    }

    /**
     * test cache line is expired when both topn&histogram were null
     */
    @Test
    public void testExpire2() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        StatisticManager.CacheLine c =
            new StatisticManager.CacheLine(1, unixTimeStamp(), unixTimeStamp());
        assert c.getHistogramMap().isEmpty();
        assert c.getTopNColumns().size() == 0;
        Assert.assertTrue(c.hasExpire());
    }

    @Test
    public void testAbnormalByEmptyHistogram() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        StatisticManager.CacheLine c = new StatisticManager.CacheLine(1, unixTimeStamp(), unixTimeStamp());
        TopN topN = new TopN(DataTypes.DateType, 1.0);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-10-1")), 3);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-11-1")), 5);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-12-1")), 11);

        topN.buildDefault(3, 1);
        c.setTopN("test_col", topN);
        c.setLastModifyTime(System.currentTimeMillis() / 1000);
        assert c.getHistogramMap().isEmpty();
        assert c.getTopNColumns().size() > 0;
        Assert.assertTrue(!c.hasExpire());
    }

    @Test
    public void testAbnormalByEmptyTopN() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        StatisticManager.CacheLine c = new StatisticManager.CacheLine(1, unixTimeStamp(), unixTimeStamp());
        Histogram histogram = new Histogram(7, new IntegerType(), 1);
        Integer[] list = new Integer[10];
        list[0] = 1;
        list[1] = 1;
        list[2] = 1;
        list[3] = 1;
        list[4] = 3;
        list[5] = 3;
        list[6] = 3;
        list[7] = 3;
        list[8] = 3;
        list[9] = 3;
        histogram.buildFromData(list);
        c.setHistogram("test_col", histogram);
        c.setLastModifyTime(System.currentTimeMillis() / 1000);
        assert c.getHistogramMap() != null;
        assert c.getTopNColumns().size() == 0;
        Assert.assertTrue(!c.hasExpire());
    }

    @Test
    public void testAbnormalByEmptyTopNAndEmptyHistogram() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        StatisticManager.CacheLine c = new StatisticManager.CacheLine(1, unixTimeStamp(), unixTimeStamp());
        c.setLastModifyTime(System.currentTimeMillis());
        assert c.getHistogramMap().isEmpty();
        assert c.getTopNColumns().size() == 0;
        Assert.assertTrue(c.hasExpire());
    }

    @Test
    public void testAbnormalByExpiredOneDay() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        StatisticManager.CacheLine c = new StatisticManager.CacheLine(1, unixTimeStamp(), unixTimeStamp());
        Histogram histogram = new Histogram(7, new IntegerType(), 1);
        Integer[] list = new Integer[10];
        list[0] = 1;
        list[1] = 1;
        list[2] = 1;
        list[3] = 1;
        list[4] = 3;
        list[5] = 3;
        list[6] = 3;
        list[7] = 3;
        list[8] = 3;
        list[9] = 3;
        histogram.buildFromData(list);
        c.setHistogram("test_col", histogram);

        TopN topN = new TopN(DataTypes.DateType, 1.0);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-10-1")), 3);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-11-1")), 5);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-12-1")), 11);

        topN.buildDefault(3, 1);
        c.setTopN("test_col", topN);

        int statisticTime = InstConfUtil.getInt(BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME);
        c.setLastModifyTime(System.currentTimeMillis() / 1000 - statisticTime - 24 * 60 * 60);
        assert c.getHistogramMap() != null;
        assert c.getTopNColumns().size() > 0;
        Assert.assertTrue(!c.hasExpire());
    }

    @Test
    public void testAbnormalByExpiredThreeDay() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        StatisticManager.CacheLine c = new StatisticManager.CacheLine(1, unixTimeStamp(), unixTimeStamp());
        Histogram histogram = new Histogram(7, new IntegerType(), 1);
        Integer[] list = new Integer[10];
        list[0] = 1;
        list[1] = 1;
        list[2] = 1;
        list[3] = 1;
        list[4] = 3;
        list[5] = 3;
        list[6] = 3;
        list[7] = 3;
        list[8] = 3;
        list[9] = 3;
        histogram.buildFromData(list);
        c.setHistogram("test_col", histogram);

        TopN topN = new TopN(DataTypes.DateType, 1.0);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-10-1")), 3);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-11-1")), 5);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-12-1")), 11);

        topN.buildDefault(3, 1);
        c.setTopN("test_col", topN);

        int statisticTime = InstConfUtil.getInt(BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME);
        c.setLastModifyTime(System.currentTimeMillis() / 1000 - statisticTime - 3 * 24 * 60 * 60);
        assert c.getHistogramMap() != null;
        assert c.getTopNColumns().size() > 0;
        Assert.assertTrue(c.hasExpireForCollection());
    }

    @Test
    public void testJsonSerialize() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);

        StatisticManager.CacheLine c =
            new StatisticManager.CacheLine(11111L, unixTimeStamp(), unixTimeStamp());
        Histogram histogram = new Histogram(7, new IntegerType(), 1);
        Integer[] list = new Integer[10];
        list[0] = 1;
        list[1] = 1;
        list[2] = 1;
        list[3] = 1;
        list[4] = 3;
        list[5] = 3;
        list[6] = 3;
        list[7] = 3;
        list[8] = 3;
        list[9] = 3;
        histogram.buildFromData(list);

        c.setHistogram("col1", histogram);
        c.setCardinality("col2", 1333L);
        c.setNullCount("col1", 100L);

        TopN topN = new TopN(DataTypes.DateType, 1.0);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-10-1")), 3);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-11-1")), 5);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-12-1")), 11);

        topN.buildDefault(3, 1);

        c.setTopN("col1", topN);
        c.setSampleRate(0.13F);
        List<Set<String>> skewFlagMap = Lists.newArrayList();
        skewFlagMap.add(Sets.newHashSet("col2"));
        c.setSkewCols(skewFlagMap);

        String json = StatisticManager.CacheLine.serializeToJson(c);
        System.out.println(json);

        StatisticManager.CacheLine cacheLine = StatisticManager.CacheLine.deserializeFromJson(json);

        assert cacheLine.getRowCount() == 11111L;
        assert cacheLine.getHistogramMap().get("col1").rangeCount(0, true, 3, false) == 4;
        assert cacheLine.getTopN("col1").rangeCount("2001-10-15", true, "2001-11-29", false) == 5;
        assert cacheLine.getSkewCols().size() == 1;
    }

    @Test
    public void testExpireForCollection() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        int expiredTime = InstConfUtil.getInt(ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME);
        StatisticManager.CacheLine c =
            new StatisticManager.CacheLine(1, unixTimeStamp() - expiredTime - 1, unixTimeStamp());

        Assert.assertTrue(c.hasExpireForCollection());

        c.setRowCount(10L);

        Assert.assertTrue(c.hasExpireForCollection());

        c.setLastModifyTime(unixTimeStamp() - expiredTime + 100);
        Assert.assertTrue(!c.hasExpireForCollection());
    }

    @Test
    public void testSerializeToJson() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        StatisticManager.CacheLine c = new StatisticManager.CacheLine(1, unixTimeStamp(), unixTimeStamp());
        System.out.println(StatisticManager.CacheLine.serializeToJson(c));
    }

    @Test
    public void testRowcount() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        StatisticManager.CacheLine cacheLine = buildCacheLine();
        assert cacheLine.getRowCount() == 11111L;
        cacheLine.addUpdateRowCount(10);
        assert cacheLine.getRowCount() == 11111L;
    }

    @Test
    public void testResetUpdateRowCount() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        StatisticManager.CacheLine cacheLine = buildCacheLine();
        assert cacheLine.getRowCount() == 11111L;
        long update = cacheLine.addUpdateRowCount(10);
        assert cacheLine.getRowCount() == 11111L;
        assert update == 11121L;
        cacheLine.resetUpdateRowCount();
        update = cacheLine.addUpdateRowCount(0);
        assert update == 11111L;
    }

    @NotNull
    private StatisticManager.CacheLine buildCacheLine() {
        StatisticManager.CacheLine c =
            new StatisticManager.CacheLine(11111L, unixTimeStamp(), unixTimeStamp());
        Histogram histogram = new Histogram(7, new IntegerType(), 1);
        Integer[] list = new Integer[10];
        list[0] = 1;
        list[1] = 1;
        list[2] = 1;
        list[3] = 1;
        list[4] = 3;
        list[5] = 3;
        list[6] = 3;
        list[7] = 3;
        list[8] = 3;
        list[9] = 3;
        histogram.buildFromData(list);
        c.setHistogram("col1", histogram);
        c.setCardinality("col2", 1333L);
        c.setNullCount("col1", 100L);
        TopN topN = new TopN(DataTypes.DateType, 1.0);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-10-1")), 3);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-11-1")), 5);
        topN.offer(StatisticUtils.packDateTypeToLong(DataTypes.DateType, Date.valueOf("2001-12-1")), 11);
        topN.buildDefault(3, 1);
        c.setTopN("col1", topN);
        c.setSampleRate(0.13F);
        List<Set<String>> skewFlagMap = Lists.newArrayList();
        skewFlagMap.add(Sets.newHashSet("col2"));
        c.setSkewCols(skewFlagMap);
        String json = StatisticManager.CacheLine.serializeToJson(c);
        System.out.println(json);
        return StatisticManager.CacheLine.deserializeFromJson(json);
    }
}
