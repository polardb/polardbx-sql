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
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CheckGsiPrepareData;
import org.apache.calcite.rel.ddl.GenericDdl;
import org.apache.calcite.sql.SqlCheckGlobalIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * Check Global Index
 */
public class LogicalCheckGsi extends BaseDdlOperation {

    private SqlCheckGlobalIndex sqlNode;
    private CheckGsiPrepareData prepareData;

    public LogicalCheckGsi(GenericDdl genericDdl, SqlCheckGlobalIndex sqlNode) {
        super(genericDdl);
        this.sqlNode = sqlNode;
    }

    public static LogicalCheckGsi create(GenericDdl genericDdl, SqlCheckGlobalIndex sqlNode) {
        return new LogicalCheckGsi(genericDdl, sqlNode);
    }

    public SqlCheckGlobalIndex getSqlNode() {
        return this.sqlNode;
    }

    /**
     * Query table name from gsi name
     */
    private String queryTableNameOfGsi(String indexName) {
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta indexTableMeta = sm.getTable(indexName);
        if (null == indexTableMeta || !indexTableMeta.isGsi()) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE,
                String.format("GSI %s not exists", indexName));
        }

        return indexTableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public CheckGsiPrepareData prepareData(ExecutionContext ec) {
        if (this.prepareData != null) {
            return this.prepareData;
        }

        // query table name if not exists
        final SqlIdentifier indexNameId = ((SqlIdentifier) sqlNode.getIndexName());
        final String indexName = Optional.ofNullable(indexNameId).map(SqlIdentifier::getLastName).orElseThrow(
            () -> new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE));
        final SqlIdentifier tableNameId = ((SqlIdentifier) sqlNode.getTableName());
        final String schemaName = (tableNameId != null && 2 == tableNameId.names.size()) ?
            tableNameId.names.get(0) : ec.getSchemaName();
        final String tableName = Optional.ofNullable(tableNameId).map(SqlIdentifier::getLastName)
            .orElseGet(() -> queryTableNameOfGsi(indexName));

        // parameters
        final long batchSize = ec.getParamManager().getLong(ConnectionParams.GSI_CHECK_BATCH_SIZE);
        final long speedLimit = ec.getParamManager().getLong(ConnectionParams.GSI_CHECK_SPEED_LIMITATION);
        final long speedMin = ec.getParamManager().getLong(ConnectionParams.GSI_CHECK_SPEED_MIN);
        final long parallelism = ec.getParamManager().getLong(ConnectionParams.GSI_CHECK_PARALLELISM);
        final long earlyFailNumber = ec.getParamManager().getLong(ConnectionParams.GSI_EARLY_FAIL_NUMBER);
        final boolean useBinary = ec.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY);
        final Pair<SqlSelect.LockMode, SqlSelect.LockMode> lockMode = prepareLockMode();

        final String extraCmd = StringUtils.defaultIfEmpty(sqlNode.getExtraCmd(), "");

        this.prepareData = new CheckGsiPrepareData(
            indexName,
            extraCmd,
            lockMode,
            batchSize,
            speedLimit,
            speedMin,
            parallelism,
            earlyFailNumber,
            useBinary
        );
        this.prepareData.setSchemaName(schemaName);
        this.prepareData.setTableName(tableName);

        return this.prepareData;
    }

    private Pair<SqlSelect.LockMode, SqlSelect.LockMode> prepareLockMode() {
        final String extraCmd = sqlNode.getExtraCmd();
        if (LogicalCheckGsi.CorrectionType.oneOf(extraCmd)) {
            return LogicalCheckGsi.CorrectionType.lockRequest(LogicalCheckGsi.CorrectionType.of(extraCmd));
        } else if ("lock".equalsIgnoreCase(extraCmd)) {
            return new Pair<>(SqlSelect.LockMode.SHARED_LOCK, SqlSelect.LockMode.SHARED_LOCK);
        } else {
            return new Pair<>(SqlSelect.LockMode.UNDEF, SqlSelect.LockMode.UNDEF);
        }
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    /**
     * Type of check correction
     */
    public enum CorrectionType {

        BASED_ON_PRIMARY("CORRECTION_BASED_ON_PRIMARY"),
        DROP_ONLY("CORRECTION_DROP_ONLY"),
        ADD_ONLY("CORRECTION_ADD_ONLY");

        /**
         * Label of correction type
         */
        private final String label;

        CorrectionType(String label) {
            this.label = label;
        }

        public static CorrectionType of(String label) {
            if (label.equalsIgnoreCase(BASED_ON_PRIMARY.getLabel())) {
                return BASED_ON_PRIMARY;
            } else if (label.equalsIgnoreCase(DROP_ONLY.getLabel())) {
                return DROP_ONLY;
            } else if (label.equalsIgnoreCase(ADD_ONLY.getLabel())) {
                return ADD_ONLY;
            } else {
                throw new IllegalArgumentException("Unsupported CorrectionType value " + label);
            }
        }

        public static boolean oneOf(String label) {
            try {
                of(label);
                return true;
            } catch (Exception ignore) {
                return false;
            }
        }

        public static Pair<SqlSelect.LockMode, SqlSelect.LockMode> lockRequest(CorrectionType type) {
            switch (type) {
            case BASED_ON_PRIMARY:
                return new Pair<>(SqlSelect.LockMode.SHARED_LOCK, SqlSelect.LockMode.EXCLUSIVE_LOCK);

            case DROP_ONLY:
                return new Pair<>(SqlSelect.LockMode.EXCLUSIVE_LOCK, SqlSelect.LockMode.EXCLUSIVE_LOCK);

            case ADD_ONLY:
                return new Pair<>(SqlSelect.LockMode.SHARED_LOCK, SqlSelect.LockMode.SHARED_LOCK);

            default:
                throw new IllegalArgumentException("Unsupported CorrectionType " + type.name());
            }
        }

        public String getLabel() {
            return label;
        }
    }
}
