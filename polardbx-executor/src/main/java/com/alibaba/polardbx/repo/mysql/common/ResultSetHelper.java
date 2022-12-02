/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.repo.mysql.common;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultSetHelper {

    public static List<Object[]> filterOutHiddenColumns(String schemaName, String tableName, List<Object[]> rows,
                                                        ExecutionContext ec) {
        List<Object[]> result = new ArrayList<>();
        for (Object[] row: rows) {
            if (!TableColumnUtils.isHiddenColumn(ec, schemaName, tableName, String.valueOf(row[0]))) {
                result.add(row);
            }
        }
        return result;
    }

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
