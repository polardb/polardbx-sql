package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.pl.ProcedureManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.Map;

public class FetchProcedureCacheSyncAction implements ISyncAction {
    public FetchProcedureCacheSyncAction() {

    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("PROCEDURE_CACHE");
        result.addColumn("ID", DataTypes.StringType);
        result.addColumn("SCHEMA", DataTypes.StringType);
        result.addColumn("PROCEDURE", DataTypes.StringType);
        result.addColumn("SIZE", DataTypes.LongType);

        Map<String, Map<String, Long>> loadedProcedures = ProcedureManager.getInstance().getProcedureStatus();
        for (Map.Entry<String, Map<String, Long>> schemaAndProc : loadedProcedures.entrySet()) {
            for (Map.Entry<String, Long> procedure : schemaAndProc.getValue().entrySet()) {
                result.addRow(new Object[] {
                    TddlNode.getHost() + ":" + TddlNode.getPort(),
                    schemaAndProc.getKey(),
                    procedure.getKey(),
                    procedure.getValue()
                });
            }
        }
        return result;
    }
}
