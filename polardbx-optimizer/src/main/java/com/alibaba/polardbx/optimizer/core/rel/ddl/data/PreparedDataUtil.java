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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentTimeExpr;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import org.apache.calcite.sql.SqlAddColumn;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlColumnDeclaration;

import java.util.ArrayList;
import java.util.List;

public class PreparedDataUtil {

    public static boolean indexExistence(String schemaName, String tableName, String indexName) {
        final TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        if (null == tableMeta) {
            throw GeneralUtil.nestedException("Unknown table '" + tableName + "'.");
        }
        return tableMeta.getSecondaryIndexes().stream()
            .anyMatch(index -> index.getPhysicalIndexName().equalsIgnoreCase(indexName));
    }

    /**
     * Copied from DdlGmsUtils
     */
    private static boolean isDefaultCurrentTimeStampColumns(TableMeta primaryTable, SqlColumnDeclaration column) {
        if (column == null) {
            return false;
        }
        // Ref: https://dev.mysql.com/doc/refman/8.0/en/timestamp-initialization.html
        // Types: TIMESTAMP and DATETIME.
        // Function: CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(), NOW(), LOCALTIME, LOCALTIME(), LOCALTIMESTAMP, and LOCALTIMESTAMP().
        // timestamp 默认值可能在不指定的时候，自动带上current timestamp
        /**
         * Ref:
         * If the explicit_defaults_for_timestamp system variable is disabled, the first TIMESTAMP column
         * has both DEFAULT CURRENT_TIMESTAMP and ON UPDATE CURRENT_TIMESTAMP if neither is specified explicitly.
         */
        final boolean hasTimestamp = primaryTable.getPhysicalColumns().stream()
            // This without scale string just basic type.
            .anyMatch(c -> c.getField().getDataType().getStringSqlType().equalsIgnoreCase("timestamp"));
        final String[] currentTimeFunction = {
            "CURRENT_TIMESTAMP",
            "NOW",
            "LOCALTIME",
            "LOCALTIMESTAMP"
        };
        if (column.getDataType().getTypeName().getLastName().toLowerCase().contains("timestamp") ||
            column.getDataType().getTypeName().getLastName().toLowerCase().contains("datetime")) {
            if (column.getDefaultVal() != null && column.getDefaultVal()
                .getValue() instanceof SQLCurrentTimeExpr.Type) {
                return true;
            }
            if (column.getDefaultExpr() != null) {
                for (String func : currentTimeFunction) {
                    if (column.getDefaultExpr().getOperator().getName().equalsIgnoreCase(func)) {
                        return true;
                    }
                }
            }
            if (!hasTimestamp && null == column.getDefaultVal() && null == column.getDefaultExpr()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Copied from DdlGmsUtils
     */
    public static boolean isAddDefaultCurrentTimeStampColumns(SqlAlterTable alterTable, TableMeta tableMeta) {
        for (SqlAlterSpecification alter : alterTable.getAlters()) {
            if (alter instanceof SqlAddColumn) {
                final SqlAddColumn addColumns = (SqlAddColumn) alter;
                final SqlColumnDeclaration column = addColumns.getColDef();
                if (isDefaultCurrentTimeStampColumns(tableMeta, column)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Copied from DdlGmsUtils
     * <p>
     * Most add column don't need to backfill, except that the column is timestamp/datetime.
     * When backfill these columns, some other on-update columns also need to backfill, to keep consistency.
     */
    public static List<String> findNeedBackfillColumns(TableMeta primaryTable, SqlAlterTable alterTable) {
        List<String> columns = new ArrayList<>();
        for (SqlAlterSpecification alter : alterTable.getAlters()) {
            if (alter instanceof SqlAddColumn) {
                final SqlAddColumn addColumn = (SqlAddColumn) alter;
                final SqlColumnDeclaration column = addColumn.getColDef();
                if (isDefaultCurrentTimeStampColumns(primaryTable, column)) {
                    columns.add(addColumn.getColName().getLastName());
                }
            }
        }

        // Caution: All columns with on update should added.
        primaryTable.getAutoUpdateColumns().forEach(c -> columns.add(c.getName()));
        return columns;
    }
}
