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

import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.factory.InsertOverwriteJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.LogicalInsertTask;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4InsertOverwrite;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TruncateUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalInsertOverwrite;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateTableWithGsiPreparedData;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper.deSerializeTask;

/**
 * @author lijiu.lzw
 */

public class LogicalInsertOverwriteHandler extends LogicalCommonDdlHandler {

    public LogicalInsertOverwriteHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        String logicalTableName = logicalDdlPlan.getTableName();
        TableValidator.validateTableName(logicalTableName);
        TableValidator.validateTableExistence(logicalDdlPlan.getSchemaName(), logicalTableName, executionContext);
        return false;
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalInsertOverwrite logicalInsertOverwrite = (LogicalInsertOverwrite) logicalDdlPlan;

        //insert sql在这里赋值，能够复用planCache
        logicalInsertOverwrite.setInsertSql(executionContext.getOriginSql());
        logicalInsertOverwrite.prepareData(executionContext);

        TruncateTableWithGsiPreparedData truncateTableWithGsiPreparedData =
            logicalInsertOverwrite.getTruncateTableWithGsiPreparedData();

        boolean isNewPartDb = DbInfoManager.getInstance()
            .isNewPartitionDb(logicalInsertOverwrite.getTruncateTableWithGsiPreparedData().getSchemaName());

        String tmpTableSuffix = TruncateUtil.generateTmpTableRandomSuffix();

        Map<String, String> tmpIndexTableMap = new HashMap<>();

        LogicalTruncateTableHandler logicalTruncateTableHandler = new LogicalTruncateTableHandler(repo);
        LogicalCreateTable logicalCreateTable =
            logicalTruncateTableHandler.generateLogicalCreateTmpTable(logicalInsertOverwrite.getSchemaName(),
                logicalInsertOverwrite.getTableName(), tmpTableSuffix, tmpIndexTableMap, isNewPartDb,
                truncateTableWithGsiPreparedData, executionContext);

        if (isNewPartDb && logicalCreateTable.isWithGsi()) {
            List<String> tmpIndexTableNames = new ArrayList<>(
                logicalCreateTable.getCreateTableWithGsiPreparedData().getIndexTablePreparedDataMap().keySet());
            List<String> originIndexTableNames = new ArrayList<>(
                truncateTableWithGsiPreparedData.getIndexTablePreparedDataMap().keySet());
            tmpIndexTableMap = TruncateUtil.generateNewPartTmpIndexTableMap(originIndexTableNames, tmpIndexTableNames);
        }

        truncateTableWithGsiPreparedData.setTmpIndexTableMap(tmpIndexTableMap);
        truncateTableWithGsiPreparedData.setLogicalCreateTable(logicalCreateTable);
        truncateTableWithGsiPreparedData.setTmpTableSuffix(tmpTableSuffix);
        if (truncateTableWithGsiPreparedData.isHasColumnarIndex()) {
            truncateTableWithGsiPreparedData.setVersionId(DdlUtils.generateVersionId(executionContext));
        }

        return new InsertOverwriteJobFactory(truncateTableWithGsiPreparedData, executionContext.getOriginSql(),
            executionContext).create();
    }

    @Override
    protected Cursor buildResultCursor(BaseDdlOperation baseDdl, DdlJob ddlJob, ExecutionContext ec) {
        int affectRows = 0;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            DdlEngineTaskAccessor accessor = new DdlEngineTaskAccessor();
            accessor.setConnection(metaDbConn);

            Long jobId = ec.getDdlJobId();
            Long taskId = ((ExecutableDdlJob4InsertOverwrite) ddlJob).getInsertTask().getTaskId();
            DdlEngineTaskRecord record = accessor.query(jobId, taskId);
            if (record == null) {
                record = accessor.archiveQuery(jobId, taskId);
            }
            if (record != null) {
                LogicalInsertTask task = (LogicalInsertTask) deSerializeTask(record.name, record.value);
                affectRows = task.getAffectRows();
            }
        } catch (Throwable ex) {
            //在metadb获取affectRows失败，但是实际上DDL任务执行成功了
            LoggerFactory.getLogger(LogicalInsertOverwriteHandler.class)
                .warn("Insert Overwrite get AffectRows failed", ex);
        }

        return new AffectRowCursor(affectRows);
    }
}
