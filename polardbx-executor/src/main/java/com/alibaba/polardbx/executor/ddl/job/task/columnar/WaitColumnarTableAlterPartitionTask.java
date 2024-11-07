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

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@TaskName(name = "WaitColumnarTableAlterPartitionTask")
@Getter
public class WaitColumnarTableAlterPartitionTask extends BaseDdlTask {
    private static final Logger logger = LoggerFactory.getLogger(WaitColumnarTableAlterPartitionTask.class);
    public static final String ALTER_PARTITION_SUCCESS_CHECKPOINT_TYPE = "ALTER_PARTITION_SUCCESS";

    private final List<String> indexNames;
    /**
     * FOR TEST USE ONLY!
     * If set to true, ddl returns succeed right after CN finish writing metadata
     */
    private final boolean skipCheck;

    public WaitColumnarTableAlterPartitionTask(String schemaName, List<String> indexNames, boolean skipCheck) {
        super(schemaName);
        this.indexNames = indexNames;
        this.skipCheck = skipCheck;
    }

    @Override
    @SneakyThrows
    protected void beforeTransaction(ExecutionContext executionContext) {
        // wait columnar index creation to be finished
        long start = System.nanoTime();

        // Always create new metadb connection to get the latest snapshot
        try (Connection conn = Objects.requireNonNull(MetaDbDataSource.getInstance()).getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(conn);

            for (String indexName : indexNames) {
                while (true) {
                    List<ColumnarTableMappingRecord> records =
                        tableInfoManager.queryColumnarTableMapping(schemaName, indexName);
                    if (records.isEmpty()) {
                        //找不到该列存索引记录了
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                            "Columnar table mapping record not found.");
                    }
                    ColumnarTableMappingRecord record = records.get(0);

                    ColumnarTableEvolutionRecord evolutionRecord =
                        tableInfoManager.queryColumnarTableEvolutionByVersionId(record.latestVersionId);

                    List<ColumnarCheckpointsRecord> checkpointsRecords =
                        tableInfoManager.queryColumnarCheckpointsByCommitTs(evolutionRecord.commitTs);
                    if (GeneralUtil.isNotEmpty(checkpointsRecords) && checkpointsRecords.get(0).getExtra()
                        .startsWith(ALTER_PARTITION_SUCCESS_CHECKPOINT_TYPE)) {
                        // ALTER TABLE PARTITION 成功
                        break;
                    }

                    if (executionContext.getDdlContext().isInterrupted()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                            "The job '" + executionContext.getDdlContext().getJobId() + "' has been interrupted");
                    }

                    TimeUnit.MILLISECONDS.sleep(1000);
                }
            }
        }
        SQLRecorderLogger.ddlLogger.info("Wait Alter Columnar table partition task ended, cost "
            + ((System.nanoTime() - start) / 1_000_000) + " ms.");
        if (executionContext.getDdlContext().isInterrupted()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "wait alter columnar table partition task is interrupted.");
        }
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected boolean isSkipExecute() {
        return this.skipCheck;
    }

    @Override
    protected boolean isSkipRollback() {
        return this.skipCheck;
    }
}
