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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.factory.LogicalAlterDatabaseReadWriteStatusFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.LogicalAlterDatabaseSetLocalityFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterDatabase;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlAlterDatabase;
import org.apache.calcite.sql.SqlSetOption;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalAlterDatabaseHandler extends LogicalCommonDdlHandler {
    protected static final Map<String, Set<String>> supportedOptionAndValues
        = ImmutableMap.of(
        "read_only", ImmutableSet.of("false", "true"),
        "locality", new HashSet<>()
    );

    public LogicalAlterDatabaseHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterDatabase logicalAlterDatabase = (LogicalAlterDatabase) logicalDdlPlan;

        SqlAlterDatabase sqlAlterDatabase = (SqlAlterDatabase) logicalAlterDatabase.relDdl.sqlNode;
        String dbName = sqlAlterDatabase.getDbName().getSimple();

        for (SqlSetOption option : sqlAlterDatabase.getOptitionList()) {
            String optionName = option.getName().getSimple().toLowerCase();
            String value = option.getValue().toString().toLowerCase();
            if (supportedOptionAndValues.containsKey(optionName) && (supportedOptionAndValues.get(optionName).isEmpty()
                || supportedOptionAndValues.get(optionName)
                .contains(value))) {
                if (optionName.equalsIgnoreCase("read_only")) {
                    int readWriteStatus =
                        value.equalsIgnoreCase("false") ? DbInfoRecord.DB_READ_WRITE : DbInfoRecord.DB_READ_ONLY;
                    return new LogicalAlterDatabaseReadWriteStatusFactory(dbName, readWriteStatus).create();
                } else if (optionName.equalsIgnoreCase("locality")) {
                    String newLocality = value;
                    if (value.startsWith("'") && value.endsWith("'")) {
                        newLocality = value.substring(1, value.length() - 1);
                    }
                    return new LogicalAlterDatabaseSetLocalityFactory(dbName, newLocality).create();
                }
            }
        }

        return new TransientDdlJob();
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterDatabase logicalAlterDatabase = (LogicalAlterDatabase) logicalDdlPlan;

        SqlAlterDatabase sqlAlterDatabase = (SqlAlterDatabase) logicalAlterDatabase.relDdl.sqlNode;
        String dbName = sqlAlterDatabase.getDbName().getSimple();

        DbInfoManager dbInfoManager = DbInfoManager.getInstance();
        boolean sourceSchemaExists = dbInfoManager
            .getDbList()
            .stream()
            .anyMatch(schema -> schema.equalsIgnoreCase(dbName));
        if (!sourceSchemaExists) {
            throw new TddlNestableRuntimeException(
                String.format("database '%s' doesn't exist", dbName));
        } else {
            logicalAlterDatabase.setSchemaName(dbName);
        }

        for (SqlSetOption option : sqlAlterDatabase.getOptitionList()) {
            String optionName = option.getName().getSimple().toLowerCase();
            String value = option.getValue().toString().toLowerCase();
            if (!supportedOptionAndValues.containsKey(optionName) || (!supportedOptionAndValues.get(optionName)
                .contains(value) && !supportedOptionAndValues.get(optionName).isEmpty())) {
                throw new TddlNestableRuntimeException(
                    String.format("option [%s=%s] is not supported", optionName, value)
                );
            }
        }
        return false;
    }
}
