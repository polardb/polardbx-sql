package com.alibaba.polardbx.optimizer.config;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.TopN;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;

import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;

/**
 * @author fangwu
 */
public class CacheLineTest {

    @Test
    public void testExpire() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        int expiredTime = InstConfUtil.getInt(ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME);
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
        assert c.getHistogramMap() == null;
        assert c.getTopNColumns().size() == 0;
        Assert.assertTrue(c.hasExpire());
    }
}
