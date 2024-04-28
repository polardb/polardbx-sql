/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "AlterTableAddLogicalForeignKeyValidateTask")
public class AlterTableAddLogicalForeignKeyValidateTask extends BaseValidateTask {
    private String tableName;
    private Long tableVersion;
    private ForeignKeyData fk;

    private transient TableMeta tableMeta;

    @JSONCreator
    public AlterTableAddLogicalForeignKeyValidateTask(String schemaName, String tableName, ForeignKeyData fk,
                                                      Long tableVersion) {
        super(schemaName);
        this.tableName = tableName;
        this.fk = fk;
        this.tableVersion = tableVersion;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        TableValidator.validateTableExistence(schemaName, tableName, executionContext);
        GsiValidator.validateAllowDdlOnTable(schemaName, tableName, executionContext);

        this.tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);

        if (this.tableMeta == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, schemaName, tableName);
        }

        if (tableMeta.getVersion() < tableVersion.longValue()) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_META_TOO_OLD, schemaName, tableName);
        }

        Set<String> constraints = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        constraints.addAll(
            tableMeta.getForeignKeys().values().stream().map(c -> c.constraint).collect(Collectors.toList()));

        checkFkConstraintsExists(constraints, fk.constraint);
    }

    private void checkFkConstraintsExists(Set<String> constraints, String constraintName) {
        if (constraints.contains(constraintName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATE_NAME_FK_CONSTRAINT, constraintName);
        }
    }
}
