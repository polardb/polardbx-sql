package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class MetricSyncAllActionTest {
    @Test
    public void testSync() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        String testInstId = "test_inst_id";
        try (
            MockedStatic<ServerInstIdManager> serverInstIdManagerMockedStatic = mockStatic(ServerInstIdManager.class)) {
            ServerInstIdManager serverInstIdManager = mock(ServerInstIdManager.class);
            serverInstIdManagerMockedStatic.when(ServerInstIdManager::getInstance).thenReturn(serverInstIdManager);
            when(serverInstIdManager.getInstId()).thenReturn(testInstId);

            MetricSyncAllAction action = new MetricSyncAllAction();
            ResultCursor resultCursor = action.sync();

            // assert meta
            int count = 0;
            assert resultCursor.getCursorMeta().getColumnMeta(count++).getName().equals(MetricSyncAllAction.INST);
            assert resultCursor.getCursorMeta().getColumnMeta(count++).getName().equals(MetricSyncAllAction.HOST);
            assert resultCursor.getCursorMeta().getColumnMeta(count++).getName()
                .equals(MetricSyncAllAction.METRIC_FEAT_KEY);
            assert resultCursor.getCursorMeta().getColumnMeta(count++).getName()
                .equals(MetricSyncAllAction.METRIC_REAL_KEY);

            // assert data
            Row r = resultCursor.next();
            Assert.assertTrue(r.getString(0).equalsIgnoreCase(testInstId));
        }

    }
}
