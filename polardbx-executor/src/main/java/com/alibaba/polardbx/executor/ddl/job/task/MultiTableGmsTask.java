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

package com.alibaba.polardbx.executor.ddl.job.task;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.base.Preconditions;

import java.sql.Connection;
import java.util.List;

/**
 * for table binding with filestorage table,
 * meta of the two table should be changed at the same time.
 */
public abstract class MultiTableGmsTask extends BaseDdlTask {

    protected List<String> schemas;
    protected List<String> tables;

    public MultiTableGmsTask(List<String> schemas, List<String> tables) {
        super(schemas.get(0));
        Preconditions.checkArgument(schemas.size() == tables.size(),
            "Schema list should have the same length as table list");
        this.schemas = schemas;
        this.tables = tables;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
        updateTableVersion(metaDbConnection);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
        updateTableVersion(metaDbConnection);
    }

    protected abstract void executeImpl(Connection metaDbConnection, ExecutionContext executionContext);

    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
    }

    protected void updateTableVersion(Connection metaDbConnection) {
        try {
            for (int i = 0; i < schemas.size(); i++) {
                TableInfoManager.updateTableVersion4Repartition(schemas.get(i), tables.get(i), metaDbConnection);
            }

        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<String> schemas) {
        this.schemas = schemas;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }
}
