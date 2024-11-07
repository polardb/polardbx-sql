package com.alibaba.polardbx.executor.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.stats.metric.FeatureStats;

/**
 * @author fangwu
 */
public class MetricSyncAction implements ISyncAction {

    public static final String JSON_KEY = "JSON_METRIC";

    public MetricSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = buildResultCursor();
        FeatureStats fs = FeatureStats.getInstance();
        resultCursor.addRow(new Object[] {JSON.toJSONString(fs)});

        FeatureStats.reset();
        ModuleLogInfo.getInstance().logRecord(Module.METRIC, LogPattern.PROCESS_END,
            new String[] {this.getClass().getSimpleName(), ""}, LogLevel.NORMAL);
        return resultCursor;
    }

    public static ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("METRIC_SYNC_RESULT");
        resultCursor.addColumn(JSON_KEY, DataTypes.VarcharType);
        resultCursor.initMeta();
        return resultCursor;
    }
}
