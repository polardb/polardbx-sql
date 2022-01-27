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

import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlVisibility;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.DropDbInfo;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropDatabase;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDropDatabase;

/**
 * @author chenmo.cm
 */
public class LogicalDropDatabaseHandler extends HandlerCommon {

    public LogicalDropDatabaseHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalDropDatabase dropDatabase = (LogicalDropDatabase) logicalPlan;
        final SqlDropDatabase sqlDropDatabase = (SqlDropDatabase) dropDatabase.getNativeSqlNode();
        final LocalityManager localityManager = LocalityManager.getInstance();

        final String dbName = sqlDropDatabase.getDbName().getSimple();
        final DbInfoRecord dbInfo = DbInfoManager.getInstance().getDbInfo(dbName);

        boolean isDropIfExists = sqlDropDatabase.isIfExists();
        DropDbInfo dropDbInfo = new DropDbInfo();
        dropDbInfo.setDbName(dbName);
        dropDbInfo.setDropIfExists(isDropIfExists);
        dropDbInfo.setAllowDropInScale(
            executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE));
        Long socketTimeout = executionContext.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
        dropDbInfo.setSocketTimeout(socketTimeout == null ? -1 : socketTimeout);
        DbTopologyManager.dropLogicalDb(dropDbInfo);
        CdcManagerHelper.getInstance()
            .notifyDdl(dbName, null, sqlDropDatabase.getKind().name(), executionContext.getOriginSql(),
                DdlVisibility.Public, executionContext.getExtraCmds());

        // Have to clear plan cache for the system database 'polardbx' since there may be some operations
        // executed via cross schema and the corresponding outdated plan cache may be left in it.
        OptimizerContext optimizerContext = OptimizerContext.getContext(SystemDbHelper.DEFAULT_DB_NAME);
        if (optimizerContext != null) {
            PlanManager planManager = optimizerContext.getPlanManager();
            if (planManager != null) {
                planManager.cleanCache();
            }
        }

        if (dbInfo != null) {
            localityManager.deleteLocalityOfDb(dbInfo.id);
        }

        return new AffectRowCursor(new int[] {0});
    }

}
