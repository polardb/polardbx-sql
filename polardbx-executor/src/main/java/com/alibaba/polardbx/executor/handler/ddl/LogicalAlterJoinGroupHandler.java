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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterJoinGroupJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterJoinGroup;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class LogicalAlterJoinGroupHandler extends LogicalCommonDdlHandler {

    public static Logger logger = LoggerFactory.getLogger(LogicalAlterJoinGroupHandler.class);

    public LogicalAlterJoinGroupHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterJoinGroup logicalAlterJoinGroup = (LogicalAlterJoinGroup) logicalDdlPlan;
        logicalAlterJoinGroup.preparedData(executionContext);
        if (GeneralUtil.isEmpty(logicalAlterJoinGroup.getPreparedData().getTablesVersion())) {
            return new TransientDdlJob();
        }
        return AlterJoinGroupJobFactory.create(logicalAlterJoinGroup.getPreparedData(),
            executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(executionContext.getSchemaName());
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "only support this execute in auto mode database");
        }
        return false;
    }
}
