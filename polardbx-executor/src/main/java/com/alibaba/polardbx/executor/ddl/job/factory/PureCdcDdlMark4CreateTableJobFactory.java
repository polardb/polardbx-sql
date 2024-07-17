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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcCreateTableIfNotExistsMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;

import java.util.Set;

public class PureCdcDdlMark4CreateTableJobFactory extends DdlJobFactory {

    private final String schemaName;
    private final String tableName;

    public PureCdcDdlMark4CreateTableJobFactory(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    protected void validate() {
        LimitValidator.validateTableNameLength(schemaName);
        LimitValidator.validateTableNameLength(tableName);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        CdcCreateTableIfNotExistsMarkTask task = new CdcCreateTableIfNotExistsMarkTask(schemaName, tableName);
        executableDdlJob.addTask(task);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, tableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

}
