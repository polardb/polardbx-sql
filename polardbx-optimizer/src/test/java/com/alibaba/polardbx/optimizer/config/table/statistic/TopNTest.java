package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.sql.Date;

/**
 * test topn of statistic module
 */
public class TopNTest {

    /**
     * test topn interface in cacheline:
     * com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager.CacheLine#getTopN(java.lang.String)
     * com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager.CacheLine#getTopNColumns()
     * com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager.CacheLine#setTopN(java.lang.String, TopN)
     * <p>
     * test serialize topn:
     * com.alibaba.polardbx.optimizer.config.table.statistic.TopN#serializeToJson(TopN)
     * com.alibaba.polardbx.optimizer.config.table.statistic.TopN#deserializeFromJson(java.lang.String)
     */
    @Test
    public void testTopNSerialize() {
        // test null value
        Assert.assertTrue(null == TopN.serializeToJson(null));
        Assert.assertTrue(null == TopN.deserializeFromJson(null));

        StatisticManager.CacheLine cl = new StatisticManager.CacheLine();
        cl.setTopN("TesTColumn", buildTopNForTest());

        Assert.assertTrue(cl.getTopNColumns().contains("testcolumn"));
        Assert.assertTrue(cl.getTopN("testColumn") != null);

        cl.setTopN("teStColumn", null);
        Assert.assertTrue(!cl.getTopNColumns().contains("testcolumn"));
        Assert.assertTrue(cl.getTopN("testColumn") == null);
    }

    /**
     * build string type topn, only for test
     *
     * @return mock topn
     */
    private TopN buildTopNForTest() {
        return new TopN(DataTypes.StringType, 1.0);
    }

    @Test
    public void testTopNWithMultiTimeType() {
        ImmutableList.of(
                // todo test year and time type
                // DataTypes.YearType,
                // DataTypes.TimeType
                DataTypes.TimestampType,
                DataTypes.DatetimeType,
                DataTypes.DateType
            )
            .stream().forEach(t -> testTopNWithSpecifyTimeType(t));
    }

    public void testTopNWithSpecifyTimeType(DataType dataType) {
        System.out.println("test time type:" + dataType);
        TopN topN = new TopN(dataType, 1.0);
        topN.offer(StatisticUtils.packDateTypeToLong(dataType, Date.valueOf("2001-10-1")), 3);
        topN.offer(StatisticUtils.packDateTypeToLong(dataType, Date.valueOf("2001-11-1")), 5);
        topN.offer(StatisticUtils.packDateTypeToLong(dataType, Date.valueOf("2001-12-1")), 11);

        topN.build(3, 1);

        String json = TopN.serializeToJson(topN);
        System.out.println(json);
        topN = TopN.deserializeFromJson(json);
        System.out.println(TopN.serializeToJson(topN));
        long rs = topN.rangeCount("2001-09-12", true, "2001-11-12", true);
        System.out.println(rs);
        assert rs == 5;

        rs = topN.rangeCount("2001-09-12", true, null, true);
        assert rs == 16;

        rs = topN.rangeCount("1680171880", true, null, true);
        assert rs == 0;

        rs = topN.rangeCount("20011002030303", true, null, true);
        assert rs == 16;

        rs = topN.rangeCount(null, true, null, true);
        assert rs == 0;

        topN.rangeCount(20011002030303L, true, null, true);
    }
}
