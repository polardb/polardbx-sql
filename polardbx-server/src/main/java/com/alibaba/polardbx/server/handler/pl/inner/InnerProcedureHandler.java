package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.matrix.jdbc.TResultSet;
import com.alibaba.polardbx.server.QueryResultHandler;
import com.alibaba.polardbx.server.ServerConnection;
import com.google.common.collect.ImmutableMap;

import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_BACKUP;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_FLUSH;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_GENERATE_SNAPSHOTS;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_ROLLBACK;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_SET_CONFIG;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_SNAPSHOT_FILES;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.TRIGGER_SYNC_POINT_TRX;

/**
 * @author yaozhili
 */
public class InnerProcedureHandler {
    private static final ImmutableMap<String, BaseInnerProcedure> INNER_PROCEDURES;

    static {
        INNER_PROCEDURES = ImmutableMap.<String, BaseInnerProcedure>builder()
            .put(TRIGGER_SYNC_POINT_TRX, new TriggerSyncPointTrx())
            .put(COLUMNAR_FLUSH, new ColumnarFlushProcedure())
            .put(COLUMNAR_BACKUP, new ColumnarBackupProcedure())
            .put(COLUMNAR_SNAPSHOT_FILES, new ColumnarSnapshotFilesProcedure())
            .put(COLUMNAR_SET_CONFIG, new ColumnarSetConfigProcedure())
            .put(COLUMNAR_ROLLBACK, new ColumnarRollbackProcedure())
            .put(COLUMNAR_GENERATE_SNAPSHOTS, new ColumnarGenerateSnapshots())
            .build();
    }

    public static void handle(SQLCallStatement statement,
                              ServerConnection c,
                              boolean hashMore) {
        String procedureName = statement.getProcedureName().getSimpleName().toLowerCase();
        BaseInnerProcedure procedure = INNER_PROCEDURES.get(procedureName);
        if (null != procedure) {
            ArrayResultCursor cursor = new ArrayResultCursor(procedureName);
            // Execute inner procedure.
            procedure.execute(c, statement, cursor);
            // Send result.
            sendResult(c, hashMore, cursor);
        } else {
            c.writeErrMessage(ErrorCode.ERR_PROCEDURE_NOT_FOUND,
                "Not found any inner procedure " + procedureName);
        }
    }

    private static void sendResult(ServerConnection c, boolean hashMore, ArrayResultCursor cursor) {
        QueryResultHandler queryResultHandler = c.createResultHandler(hashMore);
        try {
            queryResultHandler.sendSelectResult(new TResultSet(cursor, null),
                new AtomicLong(0), Long.MAX_VALUE);
        } catch (Throwable t) {
            c.writeErrMessage(ErrorCode.ERR_PROCEDURE_EXECUTE, t.getMessage());
        }
        queryResultHandler.sendPacketEnd(false);
    }
}
