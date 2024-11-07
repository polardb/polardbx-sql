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

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.checker.CciChecker;
import com.alibaba.polardbx.executor.columnar.checker.CciFastChecker;
import com.alibaba.polardbx.executor.columnar.checker.ICciChecker;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;

@TaskName(name = "CreateCheckCciTask")
@Getter
public class CreateCheckCciTask extends BaseDdlTask {
    private static final Logger logger = LoggerFactory.getLogger(CreateCheckCciTask.class);

    private final String logicalTableName;
    private final String indexName;
    private final boolean skipCheck;

    public CreateCheckCciTask(String schemaName, String logicalTableName, String indexName,
                              boolean skipCheck) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.indexName = indexName;
        this.skipCheck = skipCheck;
    }

    @Override
    @SneakyThrows
    protected void beforeTransaction(ExecutionContext executionContext) {
        if (null != executionContext.getParamManager()
            && executionContext.getParamManager().getBoolean(ConnectionParams.SKIP_CHECK_CCI_TASK)) {
            // Session variable is true, skip it.
            return;
        }

        if (InstConfUtil.getBool(ConnectionParams.SKIP_CHECK_CCI_TASK)) {
            // Global variable is true, skip it.
            return;
        }

        Runnable recover = null;
        if (executionContext.isForce2pcDuringCciCheck()) {
            recover = ExecUtils.forceAllTrx2PC();
        }

        ICciChecker checker;
        if (executionContext.isEnableCciFastChecker() && ExecUtils.canUseCciFastChecker(schemaName, indexName)) {
            checker = new CciFastChecker(schemaName, logicalTableName, indexName);
        } else {
            checker = new CciChecker(schemaName, logicalTableName, indexName);
        }

        try {
            long start = System.nanoTime();
            checker.check(executionContext, recover);
            SQLRecorderLogger.ddlLogger.info((executionContext.isEnableCciFastChecker() ? "Fast " : "")
                + "Check cci " + schemaName + "." + logicalTableName + "." + indexName
                + " cost " + (System.nanoTime() - start) / 1_000_000 + " ms.");
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                (executionContext.isEnableCciFastChecker() ? "Fast " : "")
                    + "Check cci failed, caused by " + t.getMessage());
        } finally {
            if (null != recover) {
                recover.run();
            }
        }

        List<String> reports = new ArrayList<>();
        boolean success = true;
        if (!checker.getCheckReports(reports)) {
            for (String error : reports) {
                SQLRecorderLogger.ddlLogger.error(
                    (executionContext.isEnableCciFastChecker() ? "Fast " : "")
                        + "Check cci " + logicalTableName + "." + indexName + " error: " + error);
            }
            success = false;
        }

        if (success) {
            return;
        }

        if (executionContext.isEnableCciFastChecker()) {
            // Fast checker failed, try naive checker.
            checker = new CciChecker(schemaName, logicalTableName, indexName);
            recover = null;
            if (executionContext.isForce2pcDuringCciCheck()) {
                recover = ExecUtils.forceAllTrx2PC();
            }
            try {
                long start = System.nanoTime();
                checker.check(executionContext, recover);
                SQLRecorderLogger.ddlLogger.info("Check cci " + schemaName + "." + logicalTableName
                    + "." + indexName + " cost " + (System.nanoTime() - start) / 1_000_000 + " ms.");
            } catch (Throwable t) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    (executionContext.isEnableCciFastChecker() ? "Fast " : "")
                        + "Check cci failed, caused by " + t.getMessage());
            } finally {
                if (null != recover) {
                    recover.run();
                }
            }
            success = true;
            if (!checker.getCheckReports(reports)) {
                for (String error : reports) {
                    SQLRecorderLogger.ddlLogger.error(
                        (executionContext.isEnableCciFastChecker() ? "Fast " : "")
                            + "Check cci " + logicalTableName + "." + indexName + " error: " + error);
                }
                success = false;
            }
        }

        if (!success) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, reports.get(0));
        }
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
