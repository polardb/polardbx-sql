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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.PlParameterAccessor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.ProcedureAccessor;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.handler.DropDatabaseHandlerCommon;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.BaselineInvalidateSchemaSyncAction;
import com.alibaba.polardbx.executor.sync.DropDbRelatedProcedureSyncAction;
import com.alibaba.polardbx.executor.sync.GsiStatisticsSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.DropDbInfo;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropDatabase;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDropDatabase;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;
import static com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils.isChangeSetProcedure;

/**
 * @author chenmo.cm
 */
public class LogicalDropDatabaseHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(LogicalDropDatabaseHandler.class);

    public LogicalDropDatabaseHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        return handleByGms(logicalPlan, executionContext);
    }

    public Cursor handleByGms(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalDropDatabase dropDatabase = (LogicalDropDatabase) logicalPlan;
        final SqlDropDatabase sqlDropDatabase = (SqlDropDatabase) dropDatabase.getNativeSqlNode();
        final LocalityManager localityManager = LocalityManager.getInstance();

        final String dbName = sqlDropDatabase.getDbName().getSimple();
        final DbInfoRecord dbInfo = DbInfoManager.getInstance().getDbInfo(dbName);

        Long connId = executionContext.getConnId();

        final ITimestampOracle timestampOracle =
            executionContext.getTransaction().getTransactionManagerUtil().getTimestampOracle();
        long ts = timestampOracle.nextTimestamp();

        DbTopologyManager.checkRefForeignKeyWhenDropDatabase(dbName);

        boolean isDropIfExists = sqlDropDatabase.isIfExists();
        DropDbInfo dropDbInfo = new DropDbInfo();
        boolean isImportDatabase = executionContext.getParamManager().getBoolean(ConnectionParams.IMPORT_DATABASE);
        if (isImportDatabase) {
            dropDbInfo.setReservePhyDb(true);
        }

        dropDbInfo.setDbName(dbName);
        dropDbInfo.setDropIfExists(isDropIfExists);
        dropDbInfo.setAllowDropForce(
            executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE)
                || executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_DROP_DATABASE_FORCE));
        dropDbInfo.setTs(ts);
        Long socketTimeout = executionContext.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
        dropDbInfo.setSocketTimeout(socketTimeout == null ? -1 : socketTimeout);
        dropDbInfo.setVersionId(DdlUtils.generateVersionId(executionContext));
        dropDbInfo.setConnId(connId);
        dropDbInfo.setTraceId(executionContext.getTraceId());

        DbTopologyManager.dropLogicalDb(dropDbInfo);
        CdcManagerHelper.getInstance()
            .notifyDdl(dbName, null, sqlDropDatabase.getKind().name(), executionContext.getOriginSql(),
                null, CdcDdlMarkVisibility.Public, buildExtendParameter(executionContext));

        SyncManagerHelper.syncWithDefaultDB(new BaselineInvalidateSchemaSyncAction(dbName), SyncScope.ALL);

        if (dbInfo != null) {
            localityManager.deleteLocalityOfDb(dbInfo.id);
        }

        dropGsiStatistic(dbName);

        dropRelatedProcedures(dbName);

        return new AffectRowCursor(new int[] {0});
    }

    private void dropGsiStatistic(String dbName) {
        SyncManagerHelper.sync(new GsiStatisticsSyncAction(dbName, null, null, GsiStatisticsSyncAction.DELETE_SCHEMA),
            SyncScope.ALL);
    }

    private void dropRelatedProcedures(String dbName) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            dropProcedureInfo(connection, dbName);
            dropProcedureParamsInfo(connection, dbName);

            // sync unregister procedure info
            SyncManagerHelper.sync(new DropDbRelatedProcedureSyncAction(dbName),
                TddlConstants.INFORMATION_SCHEMA,
                SyncScope.ALL);
        } catch (Exception ex) {
            logger.error("Drop related procedure failed ,schema is " + dbName, ex);
        }
    }

    public void dropProcedureInfo(Connection connection, String dbName) {
        ProcedureAccessor accessor = new ProcedureAccessor();
        accessor.setConnection(connection);
        accessor.dropRelatedProcedure(dbName);
    }

    public void dropProcedureParamsInfo(Connection connection, String dbName) {
        PlParameterAccessor plParameterAccessor = new PlParameterAccessor();
        plParameterAccessor.setConnection(connection);
        plParameterAccessor.dropRelatedParams(dbName);
    }

    private void validateChangeSetExists(String schemaName, ExecutionContext ec) {
        if (!isChangeSetProcedure(ec)) {
            return;
        }
        Map<String, List<String>> storageInstIdGroupNames = StatsUtils.queryAllGroupNameAndInstId(schemaName);

        for (Map.Entry<String, List<String>> item : storageInstIdGroupNames.entrySet()) {
            String groupName = item.getValue().get(0);
            Set<String> groupNames = new TreeSet<>(String::compareToIgnoreCase);
            groupNames.addAll(item.getValue());

            List<List<Object>> res;
            try {
                res = ChangeSetUtils.queryGroup(ec, schemaName, groupName, ChangeSetUtils.SQL_CALL_CHANGESET_STATS);
            } catch (Throwable e) {
                continue;
            }

            if (res != null && !res.isEmpty()) {
                for (List<Object> row : res) {
                    if (groupNames.contains(row.get(0).toString() + "_group")) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                            "Drop database not allowed whose table has changeset.");
                    }
                }
            }
        }
    }
}
