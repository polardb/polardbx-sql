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
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.MultiTableGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "AlterMultiTablesHideMetaTask")
public class AlterMultiTablesHideMetaTask extends MultiTableGmsTask {

    private List<List<String>> columnNames;
    private List<List<String>> indexNames;

    @JSONCreator
    public AlterMultiTablesHideMetaTask(List<String> schemas, List<String> tables, List<List<String>> columnNames,
                                        List<List<String>> indexNames) {
        super(schemas, tables);
        Preconditions.checkArgument(schemas.size() == columnNames.size());
        Preconditions.checkArgument(schemas.size() == indexNames.size());
        this.columnNames = columnNames;
        this.indexNames = indexNames;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        for (int i = 0; i < schemas.size(); i++) {
            TableMetaChanger.hideTableMeta(metaDbConnection, schemas.get(i), tables.get(i),
                columnNames.get(i), indexNames.get(i));
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        for (int i = 0; i < schemas.size(); i++) {
            TableMetaChanger.showTableMeta(metaDbConnection, schemas.get(i), tables.get(i),
                columnNames.get(i), indexNames.get(i));
        }
    }
}
