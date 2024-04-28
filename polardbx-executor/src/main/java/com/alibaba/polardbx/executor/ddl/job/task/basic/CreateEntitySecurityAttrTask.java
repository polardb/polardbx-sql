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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.lbac.LBACSecurityEntity;
import com.alibaba.polardbx.gms.lbac.accessor.LBACEntityAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author pangzhaoxing
 */

@TaskName(name = "CreateEntitySecurityAttrTask")
public class CreateEntitySecurityAttrTask extends BaseGmsTask {

    List<LBACSecurityEntity> esaList;

    @JSONCreator
    public CreateEntitySecurityAttrTask(String schemaName, String logicalTableName, List<LBACSecurityEntity> esaList) {
        super(schemaName, logicalTableName);
        this.esaList = esaList;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {

        LBACEntityAccessor esaAccessor = new LBACEntityAccessor();
        esaAccessor.setConnection(metaDbConnection);
        for (LBACSecurityEntity esa : esaList) {
            esaAccessor.replace(esa);
        }
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        super.onExecutionSuccess(executionContext);
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getLBACSecurityDataId(),
                conn);
            // wait for all cn to load metadb
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getLBACSecurityDataId());
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        LBACEntityAccessor esaAccessor = new LBACEntityAccessor();
        esaAccessor.setConnection(metaDbConnection);
        for (LBACSecurityEntity esa : esaList) {
            esaAccessor.deleteByKeyAndType(esa);
        }
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        super.onRollbackSuccess(executionContext);
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getLBACSecurityDataId(),
                conn);
            // wait for all cn to load metadb
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getLBACSecurityDataId());
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public List<LBACSecurityEntity> getEsaList() {
        return esaList;
    }

}
