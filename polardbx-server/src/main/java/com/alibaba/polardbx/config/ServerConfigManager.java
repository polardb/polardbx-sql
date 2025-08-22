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
import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.executor.ddl.sync.JobRequest;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.matrix.config.MatrixConfigHolder;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.matrix.jdbc.utils.TDataSourceInitUtils;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;

import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.server.conn.InnerConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    public Pair<String, String> findGroupByUniqueId(long uniqueId, Map<String, List<String>> schemaAndGroupsCache) {
        for (SchemaConfig schemaConfig : CobarServer.getInstance().getConfig().getSchemas().values()) {
            // Only search in the loaded schema
            String schemaName = schemaConfig.getName();
            if (schemaConfig.getDataSource().isInited()) {
                for (Group group : schemaConfig.getDataSource().getConfigHolder().getMatrix().getGroups()) {
                    String groupName = group.getName();
                    if (IServerConfigManager.getGroupUniqueId(schemaName, groupName) == uniqueId) {
                        return Pair.of(schemaName, groupName);
                    }
                }
            } else {
                List<String> groups = schemaAndGroupsCache.computeIfAbsent(schemaName, k -> {
                    // This schema does not init, find groups through metadb.
                    List<String> groups0 = new ArrayList<>();
                    DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
                    if (null != MetaDbDataSource.getInstance()) {
                        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
                            dbGroupInfoAccessor.setConnection(conn);
                            List<DbGroupInfoRecord> dbGroupInfoRecords = dbGroupInfoAccessor.queryDbGroupByDbName(k);
                            for (DbGroupInfoRecord dbGroupInfoRecord : dbGroupInfoRecords) {
                                groups0.add(dbGroupInfoRecord.groupName.toUpperCase());
                            }
                        } catch (Throwable t) {
                            logger.error(t);
                            return null;
                        }
                    }
                    return groups0.isEmpty() ? null : groups0;
                });

                if (null != groups) {
                    for (String group : groups) {
                        if (IServerConfigManager.getGroupUniqueId(schemaName, group) == uniqueId) {
                            // Found expected group but it does not init.
                            return Pair.of(schemaName, null);
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public List<ExecutionContext.ErrorMessage> performAsyncDDLJob(Job job, String schemaName, Object jobRequest) {
        MatrixConfigHolder matrixConfigHolder = getMatrixConfigHolder(schemaName);
        return matrixConfigHolder.performAsyncDDLJob(job, schemaName, (JobRequest) jobRequest);
    }

    @Override
    public DdlContext restoreDDL(String schemaName, Long jobId) {
        MatrixConfigHolder matrixConfigHolder = getMatrixConfigHolder(schemaName);
        return matrixConfigHolder.restoreDDL(schemaName, jobId);
    }

    @Override
    public void remoteExecuteDdlTask(String schemaName, Long jobId, Long taskId) {
        MatrixConfigHolder matrixConfigHolder = getMatrixConfigHolder(schemaName);
        matrixConfigHolder.remoteExecuteDdlTask(schemaName, jobId, taskId);
    }

    @Override
    public long submitRebalanceDDL(String schemaName, String sql) {
        MatrixConfigHolder matrixConfigHolder = getMatrixConfigHolder(schemaName);
        return matrixConfigHolder.submitRebalanceDDL(schemaName, sql);
    }

    @Override
    public long submitSubDDL(String schemaName, DdlContext parentDdlContext, long parentJobId, long parentTaskId,
                             boolean forRollback,
                             String sql, ParamManager paramManager) {
        MatrixConfigHolder matrixConfigHolder = getMatrixConfigHolder(schemaName);
        return matrixConfigHolder.submitSubDDL(schemaName, parentDdlContext, parentJobId, parentTaskId, forRollback,
            sql);
    }

    @Override
    public void executeBackgroundSql(String sql, String schema, InternalTimeZone timeZone) {
        MatrixConfigHolder matrixConfigHolder = getMatrixConfigHolder(schema);
        matrixConfigHolder.executeBackgroundSql(sql, schema, timeZone);
    }

    @Override
    public List<Map<String, Object>> executeQuerySql(String sql, String schema, InternalTimeZone timeZone) {
        MatrixConfigHolder matrixConfigHolder = getMatrixConfigHolder(schema);
        return matrixConfigHolder.executeQuerySql(sql, schema, timeZone);
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

    @Override
    public List<String> getLoadedSchemas() {
        final List<String> loadedSchemas = new ArrayList<>();
        for (SchemaConfig schemaConfig : CobarServer.getInstance().getConfig().getSchemas().values()) {
            if (schemaConfig.getDataSource().isInited()) {
                loadedSchemas.add(schemaConfig.getName());
            }
        }
        return loadedSchemas;
    }

    @Override
    public Object getTransConnection(String schema) throws SQLException {
        return new InnerConnection(schema);
    }

    @Override
    public Object getTransConnection(String schema, Map<String, Object> sessionVariables) throws SQLException {
        return new InnerConnection(schema, false, sessionVariables);
    }

    @Override
    public void closeTransConnection(Object transConn) throws SQLException {
        ((InnerConnection) transConn).close();
    }

    @Override
    public int executeBackgroundDmlByTransConnection(String sql, String schema,
                                                     InternalTimeZone timeZone,
                                                     Object transConn) {
        InnerConnection connection = (InnerConnection) transConn;
        int arows = 0;
        Exception ex = null;
        try (Statement stmt = connection.createStatement()) {
            arows = stmt.executeUpdate(sql);
            return arows;
        } catch (SQLException e) {
            ex = e;
            throw new TddlNestableRuntimeException(e);
        } finally {
            if (ex == null) {
                connection.releaseAutoSavepoint();
            }
        }

    }

    @Override
    public List<Map<String, Object>> executeBackgroundQueryByTransConnection(String sql, String schema,
                                                                             InternalTimeZone timeZone,
                                                                             Object transConn) {
        InnerConnection connection = (InnerConnection) transConn;
        List<Map<String, Object>> result = null;
        Exception ex = null;

        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            result = ExecUtils.resultSetToList(rs);
            return result;
        } catch (SQLException e) {
            ex = e;
            throw new TddlNestableRuntimeException(e);
        } finally {
            if (ex == null) {
                connection.releaseAutoSavepoint();
            }
        }
    }

    @Override
    public void transConnectionBegin(Object transConn) throws SQLException {
        InnerConnection conn = (InnerConnection) transConn;
        conn.setAutoCommit(false);
    }

    @Override
    public void transConnectionCommit(Object transConn) throws SQLException {
        InnerConnection conn = (InnerConnection) transConn;
        conn.commit();
    }

    @Override
    public void transConnectionRollback(Object transConn) throws SQLException {
        InnerConnection conn = (InnerConnection) transConn;
        conn.rollback();

    }

    private void releaseAutoSavepoint(Object transConn) {
        InnerConnection conn = (InnerConnection) transConn;
        conn.releaseAutoSavepoint();
    }

}
