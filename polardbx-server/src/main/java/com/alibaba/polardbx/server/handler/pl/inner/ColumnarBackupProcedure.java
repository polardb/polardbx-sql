package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import org.apache.calcite.sql.SqlKind;

import static com.alibaba.polardbx.common.columnar.ColumnarUtils.AddCDCMarkEvent;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_BACKUP;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.POLARDBX_INNER_PROCEDURE;

/**
 * @author lijiu
 */
public class ColumnarBackupProcedure extends BaseInnerProcedure {

    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        String sql = "call " + POLARDBX_INNER_PROCEDURE + "." + COLUMNAR_BACKUP + "()";

        Long tso = AddCDCMarkEvent(sql, SqlKind.PROCEDURE_CALL.name());

        if (tso == null || tso <= 0) {
            throw new RuntimeException(sql + " is failed, because tso: " + tso);
        }

        //返回结果
        cursor.addColumn("COMMIT_TSO", DataTypes.LongType);
        cursor.addRow(new Object[] {tso});
    }
}
