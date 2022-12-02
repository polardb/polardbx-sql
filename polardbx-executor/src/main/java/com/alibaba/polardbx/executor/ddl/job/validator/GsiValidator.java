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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class GsiValidator {

    public static void validateGsiSupport(String schemaName, ExecutionContext executionContext) {
        ParamManager paramManager = executionContext.getParamManager();

        if (!paramManager.getBoolean(ConnectionParams.ALLOW_ADD_GSI)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_ALLOW_ADD);
        }

        if (paramManager.getBoolean(ConnectionParams.STORAGE_CHECK_ON_GSI)
            && !paramManager.getBoolean(ConnectionParams.GSI_IGNORE_RESTRICTION)
            && !ExecutorContext.getContext(schemaName).getStorageInfoManager().supportXA()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_STORAGE_VERSION);
        }

        // Creating a cross-schema GSI is forbidden.
        if (!executionContext.getSchemaName().equalsIgnoreCase(schemaName)
            && !paramManager.getBoolean(ConnectionParams.GSI_IGNORE_RESTRICTION)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED,
                "Adding global index on other schema is forbidden, so please login with corresponding schema.");
        }

        validateEnableMDL(executionContext);
    }

    public static void validateCreateOnGsi(String schemaName,
                                           String indexName,
                                           ExecutionContext executionContext) {
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_ADD_GSI)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_ALLOW_ADD);
        }
        LimitValidator.validateIndexNameLength(indexName);
        TableValidator.validateTableNonExistence(schemaName, indexName, executionContext);
    }

    public static void validateEnableMDL(ExecutionContext executionContext) {
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_MDL)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                "must enable MDL when dynamic create GSI");
        }
    }

    /**
     * validate if the indexName is exist
     */
    public static void validateGsiExistence(String schemaName,
                                            String primaryTableName,
                                            String indexName,
                                            ExecutionContext executionContext) {
        // check if the indexName is already exist
        List<TableMeta> tableMetaList =
            GlobalIndexMeta.getIndex(primaryTableName, schemaName, IndexStatus.ALL, executionContext);
        for (TableMeta tableMeta : tableMetaList) {
            if (StringUtils.equalsIgnoreCase(tableMeta.getTableName(), indexName)) {
                return;
            }
        }
        String errMsg = String.format("Global Secondary Index %s doesn't exists", indexName);
        throw new TddlNestableRuntimeException(errMsg);
    }

    public static void validateAllowDdlOnTable(String schemaName,
                                               String tableName,
                                               ExecutionContext executionContext) {
        boolean allowDdlOnGsi = executionContext.getParamManager().getBoolean(ConnectionParams.DDL_ON_GSI);
        boolean isGsi = TableValidator.checkTableIsGsi(schemaName, tableName);
        if (allowDdlOnGsi || !isGsi) {
            return;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_WITH_DDL, tableName);
    }

    public static void validateAllowRenameOnTable(String schemaName,
                                                  String tableName,
                                                  ExecutionContext executionContext) {
        boolean isGsi = TableValidator.checkTableIsGsi(schemaName, tableName);
        boolean hasGsi = TableValidator.checkTableWithGsi(schemaName, tableName);

        if (hasGsi && !executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_PRIMARY_TABLE_DIRECTLY,
                tableName);
        }

        if (isGsi && !executionContext.getParamManager().getBoolean(ConnectionParams.DDL_ON_GSI)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_WITH_DDL,
                tableName);
        }
    }

    public static void validateAllowTruncateOnTable(String schemaName,
                                                    String tableName,
                                                    ExecutionContext executionContext) {
        boolean isGsi = TableValidator.checkTableIsGsi(schemaName, tableName);
        boolean hasGsi = TableValidator.checkTableWithGsi(schemaName, tableName);
        ParamManager paramManager = executionContext.getParamManager();

        if (isGsi && !paramManager.getBoolean(ConnectionParams.DDL_ON_GSI)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_WITH_DDL,
                tableName);
        }

        if (hasGsi && !executionContext.getSchemaName().equalsIgnoreCase(schemaName)
            && !paramManager.getBoolean(ConnectionParams.GSI_IGNORE_RESTRICTION)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED,
                "Truncating table with GSI on other schema is forbidden, so please login with corresponding schema.");
        }
    }

}
