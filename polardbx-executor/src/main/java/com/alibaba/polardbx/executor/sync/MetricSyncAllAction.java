package com.alibaba.polardbx.executor.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.statistic.RealStatsLog;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.stats.metric.FeatureStats;

/**
 * @author fangwu
 */
public class MetricSyncAllAction implements ISyncAction {

    public static final String INST = "INST";
    public static final String HOST = "HOST";
    public static final String METRIC_FEAT_KEY = "METRIC_FEAT_KEY";
    public static final String METRIC_REAL_KEY = "METRIC_REAL_KEY";

    public MetricSyncAllAction() {
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = buildResultCursor();
        FeatureStats fs = FeatureStats.getInstance();
        String instId = ServerInstIdManager.getInstance().getInstId();

        resultCursor.addRow(new Object[] {
            instId,
            TddlNode.getHost() + ":" + TddlNode.getPort(),
            JSON.toJSONString(fs),
            RealStatsLog.statLog()});

        ModuleLogInfo.getInstance()
            .logInfo(Module.METRIC, LogPattern.PROCESS_END,
                new String[] {this.getClass().getSimpleName(), "metric sync all"});
        return resultCursor;
    }

    public static ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("METRIC_SYNC_RESULT");
        resultCursor.addColumn(INST, DataTypes.VarcharType);
        resultCursor.addColumn(HOST, DataTypes.VarcharType);
        resultCursor.addColumn(METRIC_FEAT_KEY, DataTypes.VarcharType);
        resultCursor.addColumn(METRIC_REAL_KEY, DataTypes.VarcharType);
        resultCursor.initMeta();
        return resultCursor;
    }
}
