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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.corrector.Reporter;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.scaleout.backfill.BackfillExecutor;
import com.alibaba.polardbx.executor.scaleout.corrector.MoveTableChecker;
import com.alibaba.polardbx.executor.scaleout.corrector.MoveTableReporter;
import com.alibaba.polardbx.executor.scaleout.fastchecker.MoveTableFastChecker;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.MoveTableBackfill;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.StringUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MoveTableBackfillHandler extends HandlerCommon {

    public MoveTableBackfillHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        MoveTableBackfill backfill = (MoveTableBackfill) logicalPlan;
        String schemaName = backfill.getSchemaName();
        String logicalTable = backfill.getLogicalTableName();

        BackfillExecutor backfillExecutor = new BackfillExecutor((List<RelNode> inputs,
                                                                  ExecutionContext executionContext1) -> {
            QueryConcurrencyPolicy queryConcurrencyPolicy = getQueryConcurrencyPolicy(executionContext1);
            List<Cursor> inputCursors = new ArrayList<>(inputs.size());
            executeWithConcurrentPolicy(executionContext1, inputs, queryConcurrencyPolicy, inputCursors, schemaName);
            return inputCursors;
        });

        executionContext = clearSqlMode(executionContext.copy());

        upgradeEncoding(executionContext, schemaName, logicalTable);

        Map<String, Set<String>> sourcePhyTables = backfill.getSourcePhyTables();
        Map<String, Set<String>> targetPhyTables = backfill.getTargetPhyTables();
        Map<String, String> sourceTargetGroupMap = backfill.getSourceTargetGroup();

        int affectRows = 0;
        if (!sourcePhyTables.isEmpty()) {
            affectRows = backfillExecutor
                .backfill(schemaName, logicalTable, executionContext, sourcePhyTables, sourceTargetGroupMap);
        }

        // Check target table immediately after backfill by default.
        assert !targetPhyTables.isEmpty();
        final boolean check =
            executionContext.getParamManager().getBoolean(ConnectionParams.SCALEOUT_CHECK_AFTER_BACKFILL);
        if (check) {
            final boolean useFastChecker =
                FastChecker.isSupported(schemaName) &&
                    executionContext.getParamManager().getBoolean(ConnectionParams.SCALEOUT_BACKFILL_USE_FASTCHECKER);

            if (useFastChecker && fastCheckWithCatchEx(backfill, executionContext)) {
                return new AffectRowCursor(affectRows);
            } else {
                checkInCN(backfill, executionContext);
            }

        }
        return new AffectRowCursor(affectRows);
    }

    protected boolean fastCheckWithCatchEx(MoveTableBackfill backfill, ExecutionContext executionContext) {
        boolean fastCheckSucc = false;
        try {
            fastCheckSucc = fastCheck(backfill, executionContext);
        } catch (Throwable ex) {
            fastCheckSucc = false;
            String msg = String.format(
                "Failed to use fastChecker to check move database backFill because of throwing exceptions,  so use old checker instead");
            SQLRecorderLogger.ddlLogger.warn(msg, ex);
        }
        return fastCheckSucc;
    }

    boolean fastCheck(MoveTableBackfill backfill,
                      ExecutionContext executionContext) {
        long startTime = System.currentTimeMillis();

        String schemaName = backfill.getSchemaName();
        String logicalTable = backfill.getLogicalTableName();

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
            "FastChecker for move table, schema [{0}] logical src table [{1}] logic dst table [{2}] start",
            schemaName, logicalTable, logicalTable));
        final int fastCheckerParallelism =
            executionContext.getParamManager().getInt(ConnectionParams.SCALEOUT_FASTCHECKER_PARALLELISM);

        FastChecker fastChecker = MoveTableFastChecker
            .create(schemaName, backfill.getLogicalTableName(), backfill.getSourceTargetGroup(),
                backfill.getSourcePhyTables(),
                backfill.getTargetPhyTables(), fastCheckerParallelism, executionContext);
        boolean fastCheckResult = false;
        final int maxRetryTimes =
            executionContext.getParamManager().getInt(ConnectionParams.FASTCHECKER_RETRY_TIMES);

        int tryTimes = 0;
        while (tryTimes < maxRetryTimes && fastCheckResult == false) {
            try {
                fastCheckResult = fastChecker.check(executionContext);
            } catch (TddlNestableRuntimeException e) {
                if (StringUtils.containsIgnoreCase(e.getMessage(), "acquire lock timeout")) {
                    //if acquire lock timeout, we will retry
                    if (tryTimes < maxRetryTimes - 1) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(2000L * (1 + tryTimes));
                        } catch (InterruptedException ex) {
                            throw new TddlNestableRuntimeException(ex);
                        }
                        continue;
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                            "move table fastchecker retry exceed max times", e);
                    }
                } else {
                    //other exception, we simply throw out
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, e,
                        "move table fastchecker failed to check");
                }
            } finally {
                tryTimes += 1;
                SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                    "FastChecker for move table, schema [{0}] logical src table [{1}] logic dst table [{2}] finish, time use [{3}], check result [{4}]",
                    schemaName, logicalTable, logicalTable,
                    (System.currentTimeMillis() - startTime) / 1000.0,
                    fastCheckResult ? "pass" : "not pass")
                );
                if (!fastCheckResult) {
                    EventLogger.log(EventType.DDL_WARN, "FastChecker failed");
                }
            }
        }
        return fastCheckResult;
    }

    void checkInCN(MoveTableBackfill backfill, ExecutionContext executionContext) {
        final long batchSize =
            executionContext.getParamManager().getLong(ConnectionParams.SCALEOUT_CHECK_BATCH_SIZE);
        final long speedLimit =
            executionContext.getParamManager().getLong(ConnectionParams.SCALEOUT_CHECK_SPEED_LIMITATION);
        final long speedMin =
            executionContext.getParamManager().getLong(ConnectionParams.SCALEOUT_CHECK_SPEED_MIN);
        final long parallelism =
            executionContext.getParamManager().getLong(ConnectionParams.SCALEOUT_CHECK_PARALLELISM);
        final long earlyFailNumber =
            executionContext.getParamManager().getLong(ConnectionParams.SCALEOUT_EARLY_FAIL_NUMBER);

        String schemaName = backfill.getSchemaName();
        String logicalTable = backfill.getLogicalTableName();
        Map<String, Set<String>> sourcePhyTables = backfill.getSourcePhyTables();
        Map<String, Set<String>> targetPhyTables = backfill.getTargetPhyTables();
        Map<String, String> sourceTargetGroupMap = backfill.getSourceTargetGroup();

        Checker checker = MoveTableChecker.create(schemaName,
            logicalTable,
            logicalTable,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            SqlSelect.LockMode.UNDEF,
            SqlSelect.LockMode.UNDEF,
            executionContext,
            sourcePhyTables,
            targetPhyTables,
            sourceTargetGroupMap);
        checker.setInBackfill(true);
        if (null == executionContext.getDdlJobId() || 0 == executionContext.getDdlJobId()) {
            checker.setJobId(JOB_ID_GENERATOR.nextId());
        } else {
            checker.setJobId(executionContext.getDdlJobId());
        }

        // Run the simple check.
        final Reporter reporter = new MoveTableReporter(earlyFailNumber);
        try {
            checker.check(executionContext, reporter);
        } catch (TddlNestableRuntimeException e) {
            if (e.getMessage().contains("Too many conflicts")) {
                throw GeneralUtil
                    .nestedException(
                        "move table checker error limit exceeded. Please try to rollback/recover this job");
            } else {
                throw e;
            }
        }

        final List<CheckerManager.CheckerReport> checkerReports = reporter.getCheckerReports();
        if (!checkerReports.isEmpty()) {
            // Some error found.
            throw GeneralUtil.nestedException(
                "move table checker found error after backfill. Please try to rollback/recover this job");
        }
    }
}
