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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CheckCciPrepareData;
import org.apache.calcite.rel.ddl.GenericDdl;
import org.apache.calcite.sql.SqlCheckColumnarIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;

import java.util.Objects;
import java.util.Optional;

/**
 * Check Columnar Index
 */
public class LogicalCheckCci extends BaseDdlOperation {

    private SqlCheckColumnarIndex sqlNode;
    private CheckCciPrepareData prepareData;

    public LogicalCheckCci(GenericDdl genericDdl, SqlCheckColumnarIndex sqlNode) {
        super(genericDdl);
        this.sqlNode = sqlNode;
    }

    public static LogicalCheckCci create(GenericDdl genericDdl, SqlCheckColumnarIndex sqlNode) {
        return new LogicalCheckCci(genericDdl, sqlNode);
    }

    public CheckCciPrepareData prepareData(ExecutionContext ec) {
        if (this.prepareData != null) {
            return this.prepareData;
        }

        // Check and index name
        final SqlIdentifier indexNameId = ((SqlIdentifier) sqlNode.getIndexName());
        final String indexName = Optional
            .ofNullable(indexNameId)
            .map(SqlIdentifier::getLastName)
            .orElseThrow(() -> new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, "Index name not specified"));

        // Check primary table schema
        final SqlIdentifier tableNameId = ((SqlIdentifier) sqlNode.getTableName());
        final String schemaName = Optional
            .ofNullable(tableNameId)
            .filter(tni -> tni.names.size() == 2)
            .map(tni -> tni.names.get(0))
            .orElseGet(ec::getSchemaName);

        // Extract primary table name
        final String tableName = Optional
            .ofNullable(tableNameId)
            .map(SqlIdentifier::getLastName)
            .orElseGet(() -> {
                try {
                    return queryTableNameOfCci(schemaName, indexName);
                } catch (Exception ignored) {
                    // If table name not specified and index does not exist,
                    // throw exception in CheckCciTask latter
                }
                return "";
            });

        // Get extra cmd and lock mode
        final SqlCheckColumnarIndex.CheckCciExtraCmd extraCmd = sqlNode.getExtraCmdEnum();

        this.prepareData = new CheckCciPrepareData(
            schemaName,
            tableName,
            indexName,
            extraCmd
        );

        return this.prepareData;
    }

    /**
     * Query table name from cci name
     */
    private String queryTableNameOfCci(String schemaName, String indexName) {
        final TableMeta indexTableMeta = Objects
            .requireNonNull(OptimizerContext.getContext(schemaName))
            .getLatestSchemaManager()
            .getTable(indexName);
        if (null == indexTableMeta || !indexTableMeta.isColumnar()) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_UNKNOWN_TABLE,
                String.format("No columnar index named %s in database %s", indexName, schemaName));
        }

        return indexTableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
    }

    public SqlCheckColumnarIndex getSqlNode() {
        return this.sqlNode;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }
}
