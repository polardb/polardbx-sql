package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.scheduler.executor.trx.GenerateColumnarSnapshotScheduledJob;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

import java.sql.SQLException;

public class ColumnarGenerateSnapshots extends BaseInnerProcedure {
    @Override
    void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        cursor.addColumn("STATUS", DataTypes.StringType);
        cursor.addColumn("RESULT", DataTypes.StringType);

        StringBuffer sb = new StringBuffer();
        try {
            GenerateColumnarSnapshotScheduledJob.generateColumnarSnapshots(sb, true);
            cursor.addRow(new Object[] {"OK", sb.toString()});
        } catch (SQLException e) {
            cursor.addRow(new Object[] {"FAIL", e.getMessage()});
        }
    }
}
