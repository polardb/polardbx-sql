package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.MetricSyncAllAction;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.stats.metric.FeatureStats;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.stats.metric.FeatureStatsItem.NEW_BASELINE_NUM;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

/**
 * @author fangwu
 */
public class InformationSchemaMetricHandlerTest {

    @Test
    public void testBuildEmptyArrayResultCursor() {
        try (MockedStatic<GmsSyncManagerHelper> gmsSyncManagerHelperMockedStatic = mockStatic(
            GmsSyncManagerHelper.class)) {
            gmsSyncManagerHelperMockedStatic.when(() -> GmsSyncManagerHelper.sync(any(), any(), any(SyncScope.class)))
                .thenReturn(null);

            ArrayResultCursor cursor = MetricSyncAllAction.buildResultCursor();
            cursor = InformationSchemaMetricHandler.buildArrayResultCursor(cursor);

            assert cursor.getRows().isEmpty();
        }
    }

    @Test
    public void testBuildArrayResultCursor() {
        List<List<Map<String, Object>>> metrics = Lists.newArrayList();
        Map<String, Object> map = Maps.newHashMap();
        map.put(MetricSyncAllAction.HOST, "host");
        map.put(MetricSyncAllAction.INST, "instId");
        map.put(MetricSyncAllAction.METRIC_REAL_KEY, "test_key:val1,test_key2:33");
        map.put(MetricSyncAllAction.METRIC_FEAT_KEY, FeatureStats.serialize(FeatureStats.build()));

        metrics.add(Lists.newArrayList(map));

        try (MockedStatic<GmsSyncManagerHelper> gmsSyncManagerHelperMockedStatic = mockStatic(
            GmsSyncManagerHelper.class)) {
            gmsSyncManagerHelperMockedStatic.when(() -> GmsSyncManagerHelper.sync(any(), any(), any(SyncScope.class)))
                .thenReturn(metrics);

            ArrayResultCursor cursor = MetricSyncAllAction.buildResultCursor();
            cursor = InformationSchemaMetricHandler.buildArrayResultCursor(cursor);

            boolean hasTestKey = false;
            boolean hasTestKey2 = false;
            boolean hasNewBaselineNum = false;
            for (Row row : cursor.getRows()) {
                String key = row.getString(2);
                if (key.equalsIgnoreCase(NEW_BASELINE_NUM.name())) {
                    hasNewBaselineNum = true;
                    continue;
                } else if (key.equalsIgnoreCase("test_key")) {
                    hasTestKey = true;
                    continue;
                } else if (key.equalsIgnoreCase("test_key2")) {
                    hasTestKey2 = true;
                    continue;
                }
            }

            assert hasNewBaselineNum && hasTestKey2 && hasTestKey;
        }
    }
}
