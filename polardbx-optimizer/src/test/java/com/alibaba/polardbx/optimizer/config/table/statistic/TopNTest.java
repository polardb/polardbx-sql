package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;

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
        TopN topN = new TopN(DataTypes.StringType, 1.0);
        return topN;
    }
}
