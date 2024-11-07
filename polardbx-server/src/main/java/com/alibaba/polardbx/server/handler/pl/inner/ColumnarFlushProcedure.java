package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.List;

import static com.alibaba.polardbx.common.columnar.ColumnarUtils.AddCDCMarkEvent;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_FLUSH;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.POLARDBX_INNER_PROCEDURE;

/**
 * @author lijiu
 */
public class ColumnarFlushProcedure extends BaseInnerProcedure {

    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        String sql = "call " + POLARDBX_INNER_PROCEDURE + "." + COLUMNAR_FLUSH + "()";

        //支持三种参数：
        // 1、columnar_flush()，实例级别
        // 2、columnar_flush(schemaName, tableName, indexName)，通过库名、表名、索引名匹配
        // 3、columnar_flush(indexId)，通过列存索引id匹配

        if (statement.getParameters().size() > 0) {
            long indexId = checkParameters(statement.getParameters(), statement);
            //直接将参数替换成列存索引id
            sql = "call " + POLARDBX_INNER_PROCEDURE + "." + COLUMNAR_FLUSH + "(" + indexId + ")";
        }

        Long tso = AddCDCMarkEvent(sql, SqlKind.PROCEDURE_CALL.name());

        if (tso == null || tso <= 0) {
            throw new RuntimeException(sql + " is failed, because tso: " + tso);
        }

        //返回结果
        cursor.addColumn("COMMIT_TSO", DataTypes.LongType);
        cursor.addRow(new Object[] {tso});
    }

    /**
     * 检查参数，返回正确的列存索引ID，未找到则报错
     *
     * @return 列存索引ID
     */
    private long checkParameters(List<SQLExpr> params, SQLCallStatement statement) {
        //0个参数不需要进该函数
        if (!(params.size() == 1 || params.size() == 3)) {
            throw new IllegalArgumentException(statement.toString() + " parameters is not match 0/1/3 parameters");
        }

        if (params.size() == 1) {
            //1个参数，代表列存索引ID
            if (!(params.get(0) instanceof SQLNumericLiteralExpr)) {
                throw new IllegalArgumentException(
                    statement.toString() + " first parameters need Long number when hava 1 parameters");
            }
            long indexId = ((SQLNumericLiteralExpr) params.get(0)).getNumber().longValue();

            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
                accessor.setConnection(metaDbConn);
                List<ColumnarTableMappingRecord> records = accessor.queryTableId(indexId);
                if (records.isEmpty()) {
                    throw new IllegalArgumentException(
                        statement.toString() + " indexId: " + indexId + " not found columnar index");
                }
                if (!ColumnarTableStatus.PUBLIC.name().equalsIgnoreCase(records.get(0).status)) {
                    throw new IllegalArgumentException(
                        statement.toString() + " indexId: " + indexId + " is not PUBLIC columnar index, status is "
                            + records.get(0).status);
                }
                return records.get(0).tableId;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            //3个参数，schemaName, tableName, indexName，通过库名、表名、索引名匹配
            if (!(params.get(0) instanceof SQLCharExpr)
                || !(params.get(1) instanceof SQLCharExpr)
                || !(params.get(2) instanceof SQLCharExpr)) {
                throw new IllegalArgumentException(
                    statement.toString() + " first/second/third parameters need String when hava 3 parameters");
            }

            String schemaName = ((SQLCharExpr) params.get(0)).getText();
            String tableName = ((SQLCharExpr) params.get(1)).getText();
            String indexName = ((SQLCharExpr) params.get(2)).getText();

            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
                accessor.setConnection(metaDbConn);
                List<ColumnarTableMappingRecord> records = accessor.queryBySchemaTableIndexLike(schemaName,
                    tableName, indexName + '%', ColumnarTableStatus.PUBLIC.name());
                if (records.isEmpty()) {
                    throw new IllegalArgumentException(
                        String.format(" %s.%s index like %s not found columnar index", schemaName, tableName,
                            indexName));
                }
                return records.get(0).tableId;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }
}
