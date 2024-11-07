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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.factory.DropIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.DropGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.columnar.DropColumnarIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropIndexWithGsiPreparedData;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.util.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

/**
 * DROP INDEX xxx ON xxx
 *
 * @author jicheng, guxu, moyi
 * @since 2021/07
 */
public class LogicalDropIndexHandler extends LogicalCommonDdlHandler {

    public LogicalDropIndexHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final Long versionId = DdlUtils.generateVersionId(executionContext);
        LogicalDropIndex logicalDropIndex = (LogicalDropIndex) logicalDdlPlan;
        logicalDropIndex.prepareData();
        logicalDropIndex.setDdlVersionId(versionId);

        if (logicalDropIndex.isGsi()) {
            return buildDropGsiJob(logicalDropIndex, executionContext);
        } else if (logicalDropIndex.isColumnar()) {
            return buildDropColumnarIndexJob(logicalDropIndex, executionContext);
        } else {
            return buildDropLocalIndexJob(logicalDropIndex, executionContext);
        }
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlDropIndex sqlDropIndex = (SqlDropIndex) logicalDdlPlan.getNativeSqlNode();

        final String tableName = sqlDropIndex.getOriginTableName().getLastName();
        TableValidator.validateTableName(tableName);
        TableValidator.validateTableExistence(logicalDdlPlan.getSchemaName(), tableName, executionContext);

        final String indexName = sqlDropIndex.getIndexName().getLastName();
        IndexValidator.validateIndexExistence(logicalDdlPlan.getSchemaName(), tableName, indexName);
        IndexValidator.validateDropLocalIndex(logicalDdlPlan.getSchemaName(), tableName, indexName);
        IndexValidator.validateDropPrimaryKey(indexName);

        return false;
    }

    public DdlJob buildDropLocalIndexJob(LogicalDropIndex logicalDropIndex, ExecutionContext executionContext) {
        ExecutableDdlJob dropLocalIndexJob = DropIndexJobFactory.createDropLocalIndex(
            logicalDropIndex.relDdl, logicalDropIndex.getNativeSqlNode(),
            logicalDropIndex.getDropLocalIndexPreparedDataList(),
            false,
            executionContext);

        Map<String, Set<String>> validSchema = Maps.newTreeMap(String::compareToIgnoreCase);
        if (dropLocalIndexJob != null && GeneralUtil.isNotEmpty(logicalDropIndex.getDropLocalIndexPreparedDataList())) {
            List<DdlTask> validateTasks = Lists.newArrayList();
            for (DropLocalIndexPreparedData preparedData : logicalDropIndex.getDropLocalIndexPreparedDataList()) {
                if (validSchema.computeIfAbsent(
                        preparedData.getSchemaName(), x -> Sets.newTreeSet(String::compareToIgnoreCase)).
                    add(preparedData.getTableName())) {
                    Map<String, Long> tableVersions = new HashMap<>();
                    tableVersions.put(preparedData.getTableName(), preparedData.getTableVersion());
                    validateTasks.add(new ValidateTableVersionTask(preparedData.getSchemaName(), tableVersions));
                }
            }
            dropLocalIndexJob.addSequentialTasks(validateTasks);

            dropLocalIndexJob.addTaskRelationship(Util.last(validateTasks), dropLocalIndexJob.getHead());
        }
        DdlTask renameTask = genRenameLocalIndexTask(logicalDropIndex.getRenameLocalIndexPreparedData());
        if (renameTask != null) {
            dropLocalIndexJob.appendTask(renameTask);
        }
        return dropLocalIndexJob;
    }

    public DdlJob buildDropGsiJob(LogicalDropIndex logicalDropIndex, ExecutionContext executionContext) {
        DropIndexWithGsiPreparedData dropIndexPreparedData = logicalDropIndex.getDropIndexWithGsiPreparedData();
        DropGlobalIndexPreparedData preparedData = dropIndexPreparedData.getGlobalIndexPreparedData();

        // gsi job
        ExecutableDdlJob gsiJob = DropGsiJobFactory.create(preparedData, executionContext, false, true);

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(preparedData.getPrimaryTableName(), preparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(preparedData.getSchemaName(), tableVersions);

        gsiJob.addTask(validateTableVersionTask);
        gsiJob.addTaskRelationship(validateTableVersionTask, gsiJob.getHead());

        // local index job
        ExecutableDdlJob localIndexJob = DropIndexJobFactory.createDropLocalIndex(
            logicalDropIndex.relDdl, logicalDropIndex.getNativeSqlNode(),
            dropIndexPreparedData.getLocalIndexPreparedDataList(),
            true,
            executionContext);
        if (localIndexJob != null) {
            gsiJob.appendJob(localIndexJob);
        }
        DdlTask renameTask = genRenameLocalIndexTask(logicalDropIndex.getRenameLocalIndexPreparedData());
        if (renameTask != null) {
            gsiJob.appendTask(renameTask);
        }
        return gsiJob;
    }

    public static DdlTask genRenameLocalIndexTask(RenameLocalIndexPreparedData renameIndexPreparedData) {
        // drop local index ---> rename index  级联删除 clustered gsi 上的 local index 时转成 rename
        if (renameIndexPreparedData != null) {
            String newRenameIndex = String.format(
                "/*+TDDL:cmd_extra(DDL_ON_GSI=true)*/alter table %s rename index %s to %s",
                surroundWithBacktick(renameIndexPreparedData.getTableName()),
                surroundWithBacktick(renameIndexPreparedData.getOrgIndexName()),
                surroundWithBacktick(renameIndexPreparedData.getNewIndexName()));
            String rollbackRenameIndex = String.format(
                "/*+TDDL:cmd_extra(DDL_ON_GSI=true)*/alter table %s rename index %s to %s",
                surroundWithBacktick(renameIndexPreparedData.getTableName()),
                surroundWithBacktick(renameIndexPreparedData.getNewIndexName()),
                surroundWithBacktick(renameIndexPreparedData.getOrgIndexName()));
            SubJobTask ddlTask = new SubJobTask(
                renameIndexPreparedData.getSchemaName(), newRenameIndex, rollbackRenameIndex);
            ddlTask.setParentAcquireResource(true);
            return ddlTask;
        }
        return null;
    }

    public DdlJob buildDropColumnarIndexJob(LogicalDropIndex logicalDropIndex, ExecutionContext executionContext) {
        DropIndexWithGsiPreparedData dropIndexPreparedData = logicalDropIndex.getDropIndexWithGsiPreparedData();
        DropGlobalIndexPreparedData preparedData = dropIndexPreparedData.getGlobalIndexPreparedData();

        ExecutableDdlJob gsiJob = DropColumnarIndexJobFactory.create(preparedData, executionContext, false, true);

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(preparedData.getPrimaryTableName(), preparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(preparedData.getSchemaName(), tableVersions);

        gsiJob.addTask(validateTableVersionTask);
        gsiJob.addTaskRelationship(validateTableVersionTask, gsiJob.getHead());

        return gsiJob;
    }
}
