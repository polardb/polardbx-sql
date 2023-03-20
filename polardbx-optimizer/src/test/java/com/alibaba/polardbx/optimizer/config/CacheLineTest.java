package com.alibaba.polardbx.optimizer.config;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
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

        Assert.assertTrue(c.hasExpire());

        c.setRowCount(10L);

        Assert.assertTrue(c.hasExpire());

        c.setLastModifyTime(unixTimeStamp() - expiredTime + 100);
        Assert.assertTrue(!c.hasExpire());
    }
}
