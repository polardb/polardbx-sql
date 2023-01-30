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

package com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.CreateStoredFunctionSyncAction;
import com.alibaba.polardbx.executor.sync.DropStoredFunctionSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import lombok.Getter;

@Getter
@TaskName(name = "CreateFunctionSyncTask")
public class CreateFunctionSyncTask extends BaseDdlTask {

    private String functionName;
    private String createFunctionContent;
    private boolean canPush;

    @JSONCreator
    public CreateFunctionSyncTask(String schemaName, String functionName, String createFunctionContent,
                                  boolean canPush) {
        super(schemaName);
        this.functionName = functionName;
        this.createFunctionContent = createFunctionContent;
        this.canPush = canPush;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        String tempCreateFunction = rewriteFuncContent(createFunctionContent);
        SyncManagerHelper.sync(new CreateStoredFunctionSyncAction(functionName, tempCreateFunction, canPush),
            TddlConstants.INFORMATION_SCHEMA, SyncScope.ALL);
    }

    private String rewriteFuncContent(String createFunctionContent) {
        SQLCreateFunctionStatement
            statement = (SQLCreateFunctionStatement) FastsqlUtils.parseSql(createFunctionContent).get(0);
        statement.setBlock(new SQLBlockStatement());
        return statement.toString(VisitorFeature.OutputPlOnlyDefinition);
    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        SyncManagerHelper.sync(new DropStoredFunctionSyncAction(functionName),
            TddlConstants.INFORMATION_SCHEMA, SyncScope.ALL);
    }
}

