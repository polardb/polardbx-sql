package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.MetricSyncAllAction;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaMetric;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.stats.metric.FeatureStats;
import com.alibaba.polardbx.stats.metric.FeatureStatsItem;
import com.google.common.collect.Maps;
import jdk.nashorn.internal.runtime.QuotedStringTokenizer;

import java.util.List;
import java.util.Map;

/**
 * @author fangwu
 */
public class InformationSchemaMetricHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaMetricHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaMetric;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        return buildArrayResultCursor(cursor);
    }

    public static ArrayResultCursor buildArrayResultCursor(ArrayResultCursor cursor) {
        List<List<Map<String, Object>>> metrics =
            GmsSyncManagerHelper.sync(new MetricSyncAllAction(), "polardbx", SyncScope.CURRENT_ONLY);
        if (metrics == null || metrics.isEmpty()) {
            return cursor;
        }
        for (List<Map<String, Object>> nodeRows : metrics) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                final String host = DataTypes.StringType.convertFrom(row.get(MetricSyncAllAction.HOST));
                final String instId = DataTypes.StringType.convertFrom(row.get(MetricSyncAllAction.INST));
                String feat = DataTypes.StringType.convertFrom(row.get(MetricSyncAllAction.METRIC_FEAT_KEY));
                String real = DataTypes.StringType.convertFrom(row.get(MetricSyncAllAction.METRIC_REAL_KEY));
                FeatureStats featureStats = FeatureStats.deserialize(feat);
                for (FeatureStatsItem item : FeatureStatsItem.values()) {
                    cursor.addRow(new Object[] {
                        host,
                        instId,
                        item.name(),
                        "feat",
                        featureStats.getLong(item)
                    });
                }

                for (Map.Entry<String, String> entry : decodeReal(real).entrySet()) {
                    cursor.addRow(new Object[] {
                        host,
                        instId,
                        entry.getKey(),
                        "real",
                        entry.getValue()
                    });
                }

            }
        }
        return cursor;
    }

    public static Map<String, String> decodeReal(String real) {
        Map<String, String> map = Maps.newHashMap();
        QuotedStringTokenizer tokenizer = new QuotedStringTokenizer(real, ",");

        while (tokenizer.hasMoreTokens()) {
            String kv = tokenizer.nextToken();
            String[] kvPair = kv.split(":", 2);
            map.put(kvPair[0], kvPair[1]);
        }

        return map;
    }

}

