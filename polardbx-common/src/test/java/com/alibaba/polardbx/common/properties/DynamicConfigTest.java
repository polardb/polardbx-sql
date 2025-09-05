package com.alibaba.polardbx.common.properties;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.constants.ServerVariables;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author fangwu
 */
public class DynamicConfigTest {

    final private Logger logger = LoggerFactory.getLogger(DynamicConfigTest.class);

    @Test
    public void testLoadInDegradationNum() {
        assertTrue(DynamicConfig.getInstance().getInDegradationNum() == 100L);
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.STATISTIC_IN_DEGRADATION_NUMBER, "1357");
        assertTrue(DynamicConfig.getInstance().getInDegradationNum() == 1357L);
    }

    @Test
    public void testEnableAccurateInfoSchemaTables() {
        assertTrue(DynamicConfig.getInstance().isEnableAccurateInfoSchemaTables());
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_ACCURATE_INFO_SCHEMA_TABLES, "false");
        assertFalse(DynamicConfig.getInstance().isEnableAccurateInfoSchemaTables());
    }

    @Test
    public void testBlackListConf() {
        assertTrue(DynamicConfig.getInstance().getBlacklistConf().size() == 0);
        DynamicConfig.getInstance().loadValue(null, TddlConstants.BLACK_LIST_CONF, "");
        assertTrue(DynamicConfig.getInstance().getBlacklistConf().size() == 0);

        DynamicConfig.getInstance().loadValue(null, TddlConstants.BLACK_LIST_CONF, "x1,y1");
        assertTrue(DynamicConfig.getInstance().getBlacklistConf().size() == 2);
        assertTrue(ServerVariables.isVariablesBlackList("x1"));
        assertTrue(ServerVariables.isVariablesBlackList("y1"));
        assertFalse(ServerVariables.isVariablesBlackList("y1,x1"));
    }

    @Test
    public void testSyncPointConfig() {
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_SYNC_POINT, "true");
        Assert.assertTrue(DynamicConfig.getInstance().isEnableSyncPoint());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_SYNC_POINT, "false");
        Assert.assertFalse(DynamicConfig.getInstance().isEnableSyncPoint());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.SYNC_POINT_TASK_INTERVAL, "200000");
        Assert.assertEquals(200000, DynamicConfig.getInstance().getSyncPointTaskInterval());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.SYNC_POINT_TASK_INTERVAL, "1000000");
        Assert.assertEquals(1000000, DynamicConfig.getInstance().getSyncPointTaskInterval());
    }

    @Test
    public void testShowColumnarStatusUseSubQueryConfig() {
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.SHOW_COLUMNAR_STATUS_USE_SUB_QUERY, "true");
        Assert.assertTrue(DynamicConfig.getInstance().isShowColumnarStatusUseSubQuery());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.SHOW_COLUMNAR_STATUS_USE_SUB_QUERY, "false");
        Assert.assertFalse(DynamicConfig.getInstance().isShowColumnarStatusUseSubQuery());
    }

    @Test
    public void testAllowColumnarBindMaster() {
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ALLOW_COLUMNAR_BIND_MASTER, "true");
        Assert.assertTrue(DynamicConfig.getInstance().allowColumnarBindMaster());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ALLOW_COLUMNAR_BIND_MASTER, "false");
        Assert.assertFalse(DynamicConfig.getInstance().allowColumnarBindMaster());
    }

    @Test
    public void testPreheatedCacheMaxEntries() {
        long preheatedCacheMaxEntries = DynamicConfig.getInstance().getPreheatedCacheMaxEntries();

        Assert.assertTrue(preheatedCacheMaxEntries == 4096L);

        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.PREHEATED_CACHE_MAX_ENTRIES, "16384");

        preheatedCacheMaxEntries = DynamicConfig.getInstance().getPreheatedCacheMaxEntries();

        Assert.assertTrue(preheatedCacheMaxEntries == 16384L);
    }

    @Test
    public void testDeadlockVar() {
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.DEADLOCK_DETECTION_80_FETCH_TRX_ROWS, "10000");
        Assert.assertEquals(10000, DynamicConfig.getInstance().getDeadlockDetection80FetchTrxRows());
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.DEADLOCK_DETECTION_DATA_LOCK_WAITS_THRESHOLD, "30000");
        Assert.assertEquals(30000, DynamicConfig.getInstance().getDeadlockDetectionDataLockWaitsThreshold());
    }

    @Test
    public void testShareReadviewInRc() {
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.ENABLE_SHARE_READVIEW_IN_RC, "true");
        Assert.assertTrue(DynamicConfig.getInstance().isEnableShareReadviewInRc());
        DynamicConfig.getInstance()
            .loadValue(logger, ConnectionProperties.ENABLE_SHARE_READVIEW_IN_RC, "false");
        Assert.assertFalse(DynamicConfig.getInstance().isEnableShareReadviewInRc());
    }

    @Test
    public void testColumnarSnapshotCache() {
        assertFalse(DynamicConfig.getInstance().enableColumnarSnapshotCache());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_COLUMNAR_SNAPSHOT_CACHE, "true");
        assertTrue(DynamicConfig.getInstance().enableColumnarSnapshotCache());

        assertEquals(60000, DynamicConfig.getInstance().getColumnarSnapshotCacheTtlMs());
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.COLUMNAR_SNAPSHOT_CACHE_TTL_MS, "1000");
        assertEquals(1000, DynamicConfig.getInstance().getColumnarSnapshotCacheTtlMs());
    }

    @Test
    public void testExistColumnarNodes() {
        assertTrue(!DynamicConfig.getInstance().existColumnarNodes());
        DynamicConfig.getInstance().existColumnarNodes(true);
        assertTrue(DynamicConfig.getInstance().existColumnarNodes());
    }

    @Test
    public void testMPPQueryMaxWait() {
        long mppQueryResultMaxWaitInMillis = DynamicConfig.getInstance().getMppQueryResultMaxWaitInMillis();

        Assert.assertTrue(mppQueryResultMaxWaitInMillis == 10L);

        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.MPP_QUERY_RESULT_MAX_WAIT_IN_MILLIS, "1000");

        mppQueryResultMaxWaitInMillis = DynamicConfig.getInstance().getMppQueryResultMaxWaitInMillis();

        Assert.assertTrue(mppQueryResultMaxWaitInMillis == 1000L);
    }

    public void testSwitchoverParams() {
        boolean enableSmoothSwitchover = DynamicConfig.getInstance().isEnableSmoothSwitchover();
        Assert.assertTrue(enableSmoothSwitchover);
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_SMOOTH_SWITCHOVER, "false");
        enableSmoothSwitchover = DynamicConfig.getInstance().isEnableSmoothSwitchover();
        Assert.assertFalse(enableSmoothSwitchover);

        int switchoverTimeoutMillis = DynamicConfig.getInstance().getSwitchoverTimeoutMillis();
        Assert.assertEquals(10000, switchoverTimeoutMillis);
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.SWITCHOVER_WAIT_TIMEOUT_IN_MILLIS, "30000");
        switchoverTimeoutMillis = DynamicConfig.getInstance().getSwitchoverTimeoutMillis();
        Assert.assertEquals(30000, switchoverTimeoutMillis);

        int switchoverCheckIntervalMillis = DynamicConfig.getInstance().getSwitchoverCheckIntervalMillis();
        Assert.assertEquals(100, switchoverCheckIntervalMillis);
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.SWITCHOVER_CHECK_INTERVAL_IN_MILLIS, "300");
        switchoverCheckIntervalMillis = DynamicConfig.getInstance().getSwitchoverCheckIntervalMillis();
        Assert.assertEquals(300, switchoverCheckIntervalMillis);

        boolean releaseDirtyReadConnectionWhenSwitchover =
            DynamicConfig.getInstance().isReleaseDirtyReadConnectionWhenSwitchover();
        Assert.assertTrue(releaseDirtyReadConnectionWhenSwitchover);
        DynamicConfig.getInstance()
            .loadValue(null, ConnectionProperties.RELEASE_DIRTY_READ_CONNECTION_WHEN_SWITCHOVER, "false");
        releaseDirtyReadConnectionWhenSwitchover =
            DynamicConfig.getInstance().isReleaseDirtyReadConnectionWhenSwitchover();
        Assert.assertFalse(releaseDirtyReadConnectionWhenSwitchover);

        int storageHaTaskPeriod = DynamicConfig.getInstance().getStorageHaTaskPeriod();
        Assert.assertEquals(2000, storageHaTaskPeriod);
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.STORAGE_HA_TASK_PERIOD, "3000");
        storageHaTaskPeriod = DynamicConfig.getInstance().getStorageHaTaskPeriod();
        Assert.assertEquals(3000, storageHaTaskPeriod);
    }
}
