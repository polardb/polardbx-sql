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
import com.alibaba.polardbx.executor.ddl.job.factory.RenameTablesJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRenameTables;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameTablesPreparedData;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlRenameTables;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class LogicalRenameTablesHandler extends LogicalCommonDdlHandler {

    public LogicalRenameTablesHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalRenameTables logicalRenameTables = (LogicalRenameTables) logicalDdlPlan;
        logicalRenameTables.prepareData();
        RenameTablesPreparedData renameTablesPreparedData = logicalRenameTables.getRenameTablesPreparedData();

        Map<String, Long> tableVersions = new HashMap<>();

        // get old table names
        for (DdlPreparedData item : renameTablesPreparedData.getOldTableNamePrepareDataList()) {
            tableVersions.put(item.getTableName(), item.getTableVersion());
        }

        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(logicalDdlPlan.getSchemaName(), tableVersions);

        List<Long> versionIds = new ArrayList<>();
        for (int i = 0; i < renameTablesPreparedData.getFromTableNames().size(); i++) {
            versionIds.add(DdlUtils.generateVersionId(executionContext));
        }
        ExecutableDdlJob result =
            new RenameTablesJobFactory(logicalDdlPlan.getSchemaName(), renameTablesPreparedData,
                executionContext, versionIds).create();
        result.addTask(validateTableVersionTask);
        result.addTaskRelationship(validateTableVersionTask, result.getHead());
        return result;
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlRenameTables sqlRenameTable = (SqlRenameTables) logicalDdlPlan.getNativeSqlNode();

        final String schemaName = logicalDdlPlan.getSchemaName();

        // mysql的逻辑是 先系统加锁，然后依次执行 table name to new table name，没有整体的校验
        // 这里简单模拟一下来做校验，另外对于sequence 的校验将只校验新产生的表名

        Set<String> allTableNames = TableValidator.getAllTableNames(schemaName);
        Set<String> allTableNamesTmp = new TreeSet<>(String::compareToIgnoreCase);
        allTableNamesTmp.addAll(allTableNames);

        for (Pair<SqlIdentifier, SqlIdentifier> item : sqlRenameTable.getTableNameList()) {
            String tableName = item.getKey().getLastName();
            String newTableName = item.getValue().getLastName();

            TableValidator.validateTableName(tableName);
            TableValidator.validateTableName(newTableName);
            TableValidator.validateTableNameLength(newTableName);

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

        return false;
    }
}
