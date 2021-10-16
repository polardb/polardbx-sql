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

package com.alibaba.polardbx.executor.ddl.job.validator;

import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlChangeColumn;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.calcite.util.Pair;

import java.util.List;

public class ColumnValidator {

    public static void validateColumnLimits(SqlCreateTable sqlCreateTable) {
        final List<Pair<SqlIdentifier, SqlColumnDeclaration>> colDefs = sqlCreateTable.getColDefs();

        // Check the column count.
        LimitValidator.validateColumnCount(colDefs.size());

        for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : colDefs) {
            // Check column name length.
            String columnName = null;
            if (colDef.getKey() != null) {
                columnName = colDef.getKey().getLastName();
            } else if (colDef.getValue() != null && colDef.getValue().getName() != null) {
                columnName = colDef.getValue().getName().getLastName();
            }
            LimitValidator.validateColumnNameLength(columnName);

            // Check column comment length.
            if (colDef.getValue() != null && colDef.getValue().getComment() != null) {
                LimitValidator.validateColumnComment(columnName, colDef.getValue().getComment().toValue());
            }
        }
    }

    public static void validateColumnLimits(String schemaName, String logicalTableName, SqlAlterTable sqlAlterTable) {
        final List<SqlAlterSpecification> alterItems = sqlAlterTable.getAlters();
        final List<String> addedColumns =
            LogicalAlterTable.getAlteredColumns(sqlAlterTable, SqlAlterTable.ColumnOpt.ADD);

        // Check column count.
        if (addedColumns != null && addedColumns.size() > 0) {
            LimitValidator.validateColumnCount(addedColumns.size(), schemaName, logicalTableName);
        }

        if (alterItems != null && alterItems.size() > 0) {
            for (SqlAlterSpecification alterItem : alterItems) {
                if (alterItem instanceof SqlModifyColumn) {
                    // Check new column comment only.
                    SqlModifyColumn modifyColumn = (SqlModifyColumn) alterItem;
                    String columnName = modifyColumn.getColName().getLastName();
                    SqlLiteral newColumnComment = modifyColumn.getColDef().getComment();
                    if (newColumnComment != null) {
                        LimitValidator.validateColumnComment(columnName, newColumnComment.toValue());
                    }
                } else if (alterItem instanceof SqlChangeColumn) {
                    SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;

                    // Check new column name.
                    String newColumnName = changeColumn.getNewName().getLastName();
                    LimitValidator.validateColumnNameLength(newColumnName);

                    // Check new column comment.
                    SqlLiteral newColumnComment = changeColumn.getColDef().getComment();
                    if (newColumnComment != null) {
                        LimitValidator.validateColumnComment(newColumnName, newColumnComment.toValue());
                    }
                }
            }
        }
    }

}
