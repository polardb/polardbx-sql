package com.alibaba.polardbx.repo.mysql.common;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultSetHelper {

    public static void reorgLogicalColumnOrder(String schemaName, String tableName, List<Object[]> rows,
                                               ArrayResultCursor resultCursor) {
        List<ColumnsRecord> logicalColumnsInOrder = ResultSetHelper.fetchLogicalColumnsInOrder(schemaName, tableName);
        if (logicalColumnsInOrder != null && !logicalColumnsInOrder.isEmpty()) {
            List<Object[]> newRows = new ArrayList<>();
            for (ColumnsRecord logicalColumn : logicalColumnsInOrder) {
                for (Object[] row : rows) {
                    if (TStringUtil.equalsIgnoreCase(String.valueOf(row[0]), logicalColumn.columnName)) {
                        newRows.add(row);
                        break;
                    }
                }
            }
            rows = newRows;
        }
        for (Object[] row : rows) {
            resultCursor.addRow(row);
        }
    }

    public static List<ColumnsRecord> fetchLogicalColumnsInOrder(String schemaName, String tableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            return tableInfoManager.queryColumns(schemaName, tableName);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

}
