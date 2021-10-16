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

package com.alibaba.polardbx.gms.metadb.limit;

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesExtAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;

public class LimitValidator {

    // Table Related Limits

    public static void validateTableNameLength(String logicalTableName) {
        validateIdentifierNameLength(logicalTableName, Limits.MAX_LENGTH_OF_LOGICAL_TABLE_NAME);
    }

    public static void validateTableCount(String tableSchema) {
        // Get current table count in the schema.
        int tableCount = 0;
        TablesExtAccessor accessor = new TablesExtAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            accessor.setConnection(metaDbConn);
            tableCount = accessor.count(tableSchema);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            accessor.setConnection(null);
        }
        // Check the limit.
        if (tableCount >= Limits.MAX_NUM_OF_LOGICAL_TABLES_PER_LOGICAL_DB) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "Too many logical tables (max = " + Limits.MAX_NUM_OF_LOGICAL_TABLES_PER_LOGICAL_DB
                    + " in a database)");
        }
    }

    public static void validateTableComment(String logicalTableName, String comment) {
        if (comment != null && comment.length() > Limits.MAX_LENGTH_OF_LOGICAL_TABLE_COMMENT) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "Comment for table '" + logicalTableName + "' is too long (max = "
                    + Limits.MAX_LENGTH_OF_LOGICAL_TABLE_COMMENT + ")");
        }
    }

    public static void validateTablePartitionNum(int tabPartitionsDefined, ParamManager paramManager) {
        // The limit by default or user defines.
        int maxTabPartitions = paramManager.getInt(ConnectionParams.MAX_TABLE_PARTITIONS_PER_DB);
        if (maxTabPartitions < DdlConstants.MIN_ALLOWED_TABLE_SHARDS_PER_DB
            || maxTabPartitions > DdlConstants.MAX_ALLOWED_TABLE_SHARDS_PER_DB) {
            maxTabPartitions = DdlConstants.DEFAULT_ALLOWED_TABLE_SHARDS_PER_DB;
        }
        if (tabPartitionsDefined > maxTabPartitions) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_PARTITIONS_EXCEED_LIMIT,
                String.valueOf(tabPartitionsDefined),
                String.valueOf(maxTabPartitions));
        }
    }

    // Column Related Limits

    public static void validateColumnNameLength(String columnName) {
        validateIdentifierNameLength(columnName, Limits.MAX_LENGTH_OF_COLUMN_NAME);
    }

    public static void validateColumnCount(int columnCount) {
        if (columnCount > Limits.MAX_NUM_OF_COLUMNS_PER_LOGICAL_TABLE) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "Too many columns (max = " + Limits.MAX_NUM_OF_COLUMNS_PER_LOGICAL_TABLE + ")");
        }
    }

    public static void validateColumnCount(int newColumnCount, String tableSchema, String tableName) {
        // Get current column count in the table.
        int columnCount = 0;
        ColumnsAccessor accessor = new ColumnsAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            accessor.setConnection(metaDbConn);
            columnCount = accessor.count(tableSchema, tableName);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            accessor.setConnection(null);
        }
        // Check the limit.
        validateColumnCount(columnCount + newColumnCount);
    }

    public static void validateColumnComment(String columnName, String comment) {
        if (comment != null && comment.length() > Limits.MAX_LENGTH_OF_COLUMN_COMMENT) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "Comment for field '" + columnName + "' is too long (max = "
                    + Limits.MAX_LENGTH_OF_COLUMN_COMMENT + ")");
        }
    }

    // Sequence Related Limits

    public static void validateSequenceNameLength(String sequenceName) {
        validateIdentifierNameLength(sequenceName, Limits.MAX_LENGTH_OF_SEQUENCE_NAME);
    }

    public static void validateSequenceCount(String schemaName) {
        int seqCount = 0;
        SequencesAccessor accessor = new SequencesAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            accessor.setConnection(metaDbConn);
            seqCount = accessor.count(schemaName);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            accessor.setConnection(null);
        }
        // Check the limit.
        if (seqCount > Limits.MAX_NUM_OF_SEPARATE_SEQUENCES_PER_DB) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "Too many separate sequences (max = " + Limits.MAX_NUM_OF_SEPARATE_SEQUENCES_PER_DB + ")");
        }
    }

    // Misc Limits

    public static void validateUserVariableLength(String userVariable) {
        validateIdentifierNameLength(userVariable, Limits.MAX_LENGTH_OF_USER_DEFINED_VARIABLE);
    }

    public static void validateConstraintNameLength(String constraintName) {
        validateIdentifierNameLength(constraintName, Limits.MAX_LENGTH_OF_CONSTRAINT_NAME);
    }

    public static void validateIndexNameLength(String indexName) {
        validateIdentifierNameLength(indexName, Limits.MAX_LENGTH_OF_INDEX_NAME);
    }

    //table group limits
    public static void validateTableGroupNameLength(String tableGroupName) {
        validateIdentifierNameLength(tableGroupName, Limits.MAX_LENGTH_OF_TABLE_GROUP);
    }

    // Private methods

    private static void validateIdentifierNameLength(String identifierName, int maxLength) {
        if (identifierName != null && identifierName.length() > maxLength) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "Identifier name '" + identifierName + "' is too long (max = " + maxLength + ")");
        }
    }

}
