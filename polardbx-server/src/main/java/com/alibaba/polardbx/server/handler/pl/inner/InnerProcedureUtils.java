package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;

public class InnerProcedureUtils {
    public static final String POLARDBX_INNER_PROCEDURE = "polardbx";

    //忽略大小写
    public static final String TRIGGER_SYNC_POINT_TRX = "trigger_sync_point_trx";
    public static final String COLUMNAR_FLUSH = "columnar_flush";
    public static final String COLUMNAR_BACKUP = "columnar_backup";
    public static final String COLUMNAR_SNAPSHOT_FILES = "columnar_snapshot_files";
    public static final String COLUMNAR_SET_CONFIG = "columnar_set_config";
    public static final String COLUMNAR_ROLLBACK = "columnar_rollback";
    public static final String COLUMNAR_GENERATE_SNAPSHOTS = "columnar_generate_snapshots";

    public static boolean isInnerProcedure(SQLCallStatement stmt) {
        SQLName procedureName = stmt.getProcedureName();
        if (procedureName instanceof SQLPropertyExpr) {
            String owner = ((SQLPropertyExpr) procedureName).getOwnerName();
            return POLARDBX_INNER_PROCEDURE.equalsIgnoreCase(owner);
        }
        return false;
    }
}
