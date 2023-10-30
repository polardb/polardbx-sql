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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@Getter
@TaskName(name = "RenameTablesValidateTask")
public class RenameTablesValidateTask extends BaseValidateTask {

    private List<String> oldTableNames;
    private List<String> newTableNames;

    @JSONCreator
    public RenameTablesValidateTask(String schemaName, List<String> oldTableNames, List<String> newTableNames) {
        super(schemaName);
        this.oldTableNames = oldTableNames;
        this.newTableNames = newTableNames;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        executionContext.setPhyTableRenamed(false);

        Set<String> allTableNames = TableValidator.getAllTableNames(schemaName);
        Set<String> allTableNamesTmp = new TreeSet<>(String::compareToIgnoreCase);
        allTableNamesTmp.addAll(allTableNames);

        for (int i = 0; i < oldTableNames.size(); ++i) {
            String tableName = oldTableNames.get(i);
            String newTableName = newTableNames.get(i);

            TableValidator.validateTableName(tableName);
            TableValidator.validateTableName(newTableName);
            TableValidator.validateTableNameLength(newTableName);

            TableValidator.validateRenamesTableNotContainsFk(schemaName, tableName, executionContext);

            if (!allTableNames.contains(tableName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, schemaName, tableName);
            }

            if (allTableNames.contains(newTableName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_ALREADY_EXISTS, newTableName);
            }

            allTableNames.remove(tableName);
            allTableNames.add(newTableName);

            if (!allTableNamesTmp.contains(newTableName)) {
                TableValidator.validateTableNamesForRename(schemaName, tableName, newTableName);
            }
        }
    }

    @Override
    protected String remark() {
        StringBuilder sb = new StringBuilder();
        sb.append("|");
        for (int i = 0; i < oldTableNames.size(); ++i) {
            sb.append(String.format("from %s to %s, ", oldTableNames.get(i), newTableNames.get(i)));
        }
        return sb.toString();
    }

}
