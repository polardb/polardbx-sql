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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.AlterTableEngineJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.server.DefaultServerConfigManager;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.taobao.tddl.common.privilege.PrivilegePoint;

public class LogicalAlterTableEngineHandler extends LogicalCommonDdlHandler {

    private final Engine sourceEngine;
    private final Engine targetEngine;

    public LogicalAlterTableEngineHandler(IRepository repo, Engine sourceEngine,
                                          Engine targetEngine) {
        super(repo);
        this.sourceEngine = sourceEngine;
        this.targetEngine = targetEngine;
    }

    @Override
    public DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final String schemaName = logicalDdlPlan.getSchemaName();
        final String primaryTableName = logicalDdlPlan.getTableName();

        PolarPrivilegeUtils.checkPrivilege(schemaName, primaryTableName, PrivilegePoint.ALTER, executionContext);
        PolarPrivilegeUtils.checkPrivilege(schemaName, primaryTableName, PrivilegePoint.DROP, executionContext);

        if (Engine.isFileStore(targetEngine)) {
            // todo
            throw GeneralUtil.nestedException("unsupported");
        }

        // check primary key
        TableMeta originalTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTableWithNull(primaryTableName);
        if (!originalTableMeta.isHasPrimaryKey()) {
            throw GeneralUtil.nestedException("Table must have primary key");
        }

        // load data
        return new AlterTableEngineJobFactory(schemaName, primaryTableName, sourceEngine, targetEngine)
            .create(true);
    }

    private void executeBackgroundSql(String sql, String schemaName, InternalTimeZone timeZone) {
        IServerConfigManager serverConfigManager = getServerConfigManager();
        serverConfigManager.executeBackgroundSql(sql, schemaName, timeZone);
    }

    private IServerConfigManager getServerConfigManager() {
        IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
        if (serverConfigManager == null) {
            serverConfigManager = new DefaultServerConfigManager(null);
        }
        return serverConfigManager;
    }
}
