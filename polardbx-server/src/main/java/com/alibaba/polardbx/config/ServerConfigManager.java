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

package com.alibaba.polardbx.config;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.matrix.config.MatrixConfigHolder;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.matrix.jdbc.utils.TDataSourceInitUtils;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext.ErrorMessage;

import java.util.List;

public class ServerConfigManager implements IServerConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(ServerConfigManager.class);

    protected CobarServer server;

    public ServerConfigManager(CobarServer server) {
        this.server = server;
    }

    @Override
    public TDataSource getAndInitDataSourceByDbName(String dbName) {

        boolean allowCrossDbQuery = CobarServer.getInstance().getConfig().getSystem().isAllowCrossDbQuery();
        if (!allowCrossDbQuery) {
            String defaultSchemaName = DefaultSchema.getSchemaName();
            if (defaultSchemaName != null && !defaultSchemaName.toLowerCase().equals(dbName.toLowerCase())) {
                return null;
            }
        }

        // Fetch loaded SchemaConfig by dbName
        SchemaConfig schema = CobarServer.getInstance().getConfig().getSchemas().get(dbName);

        if (schema != null) {
            TDataSource ds = schema.getDataSource();
            if (ds != null) {
                Throwable dsInitEx = TDataSourceInitUtils.initDataSource(ds);
                if (dsInitEx != null) {
                    logger.warn(String.format(
                        "Failed to load new TDataSource for the schema[%s], so ignore the schema",
                        dbName),
                        dsInitEx);
                    return null;
                }
                return ds;
            } else {
                return null;
            }
        }

        return null;
    }

    @Override
    public Pair<String, String> findGroupByUniqueId(long uniqueId) {
        for (SchemaConfig schemaConfig : CobarServer.getInstance().getConfig().getSchemas().values()) {
            // Only search in the loaded schema
            if (schemaConfig.getDataSource().isInited()) {
                String schemaName = schemaConfig.getName();
                for (Group group : schemaConfig.getDataSource().getConfigHolder().getMatrix().getGroups()) {
                    String groupName = group.getName();
                    if (IServerConfigManager.getGroupUniqueId(schemaName, groupName) == uniqueId) {
                        return Pair.of(schemaName, groupName);
                    }
                }
            }
        }
        return null;
    }

    @Override
    public DdlContext restoreDDL(String schemaName, Long jobId) {
        MatrixConfigHolder matrixConfigHolder = getMatrixConfigHolder(schemaName);
        return matrixConfigHolder.restoreDDL(schemaName, jobId);
    }

    @Override
    public long submitRebalanceDDL(String schemaName, String sql) {
        MatrixConfigHolder matrixConfigHolder = getMatrixConfigHolder(schemaName);
        return matrixConfigHolder.submitRebalanceDDL(schemaName, sql);
    }

    @Override
    public long submitSubDDL(String schemaName, long parentJobId, long parentTaskId, boolean forRollback, String sql) {
        MatrixConfigHolder matrixConfigHolder = getMatrixConfigHolder(schemaName);
        return matrixConfigHolder.submitSubDDL(schemaName, parentJobId, parentTaskId, forRollback, sql);
    }

    @Override
    public void executeBackgroundSql(String sql, String schema, InternalTimeZone timeZone) {
        MatrixConfigHolder matrixConfigHolder = getMatrixConfigHolder(schema);
        matrixConfigHolder.executeBackgroundSql(sql, schema, timeZone);
    }

    private MatrixConfigHolder getMatrixConfigHolder(String schemaName) {

        TDataSource dataSource = getAndInitDataSourceByDbName(schemaName);
        if (dataSource != null) {
            if (!dataSource.isInited()) {
                dataSource.init();
            }
            MatrixConfigHolder matrixConfigHolder = dataSource.getConfigHolder();
            if (matrixConfigHolder != null && matrixConfigHolder.isInited()) {
                return matrixConfigHolder;
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED, "MatrixConfigHolder is not ready");
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED, "TDataSource is not ready");
        }
    }

}
