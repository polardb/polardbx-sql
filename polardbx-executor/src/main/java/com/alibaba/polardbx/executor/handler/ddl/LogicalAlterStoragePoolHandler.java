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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.storagepool.AlterStoragePoolAddNodeJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.storagepool.AlterStoragePoolDrainNodeJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.storagepool.CreateStoragePoolJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.handler.LogicalRebalanceHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterStoragePool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class LogicalAlterStoragePoolHandler extends LogicalCommonDdlHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LogicalRebalanceHandler.class);

    public LogicalAlterStoragePoolHandler(IRepository repo) {
        super(repo);
    }

    public static final String DRAIN = "DRAIN";

    public static final String APPEND = "APPEND";

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterStoragePool logicalAlterStoragePool =
            (LogicalAlterStoragePool) logicalDdlPlan;
        boolean enableOperateSubJob =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_OPERATE_SUBJOB);
        boolean validateStorageInstIdle =
            !(executionContext.getParamManager().getBoolean(ConnectionParams.SKIP_VALIDATE_STORAGE_INST_IDLE));
        logicalAlterStoragePool.prepareData(validateStorageInstIdle);
        if (DRAIN.equalsIgnoreCase(logicalAlterStoragePool.getPreparedData().operationType)) {
            return new AlterStoragePoolDrainNodeJobFactory(logicalAlterStoragePool.getPreparedData(),
                executionContext).create();
        } else if (APPEND.equalsIgnoreCase(logicalAlterStoragePool.getPreparedData().operationType)) {
            return new AlterStoragePoolAddNodeJobFactory(logicalAlterStoragePool.getPreparedData(),
                executionContext).create();
        } else {
            return new ExecutableDdlJob();
        }
    }

    private boolean tryGetLock(Connection conn, String lockResource) {
        try (Statement statement = conn.createStatement();
            ResultSet lockRs = statement.executeQuery("SELECT GET_LOCK('" + lockResource + "', 0) ")) {
            return lockRs.next() && lockRs.getInt(1) == 1;
        } catch (Throwable e) {
            LOG.warn("tryGetLock error", e);
            return false;
        }
    }

    private boolean releaseLock(Connection conn, String lockResource) {
        try (Statement statement = conn.createStatement();
            ResultSet lockRs = statement.executeQuery("SELECT RELEASE_LOCK('" + lockResource + "') ")) {
            return lockRs.next() && lockRs.getInt(1) == 1;
        } catch (Exception e) {
            LOG.warn("releaseLock error", e);
            return false;
        }
    }
}
