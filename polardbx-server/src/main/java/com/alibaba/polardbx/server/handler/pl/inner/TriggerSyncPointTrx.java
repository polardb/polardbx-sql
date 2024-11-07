package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.handler.SyncPointExecutor;

/**
 * @author yaozhili
 */
public class TriggerSyncPointTrx extends BaseInnerProcedure {

    @Override
    void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        cursor.addColumn("RESULT", DataTypes.StringType);

        SyncPointExecutor executor = SyncPointExecutor.getInstance();
        if (executor.execute()) {
            cursor.addRow(new Object[] {"OK"});
        } else {
            cursor.addRow(new Object[] {"FAIL"});
        }
    }
}
