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
import com.alibaba.polardbx.executor.partitionmanagement.BackfillExecutor;
import com.alibaba.polardbx.executor.partitionmanagement.corrector.AlterTableGroupChecker;
import com.alibaba.polardbx.executor.partitionmanagement.corrector.AlterTableGroupReporter;
import com.alibaba.polardbx.executor.partitionmanagement.fastchecker.AlterTableGroupFastChecker;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.AlterTableGroupBackfill;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableMap;
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
public class AlterTableGroupBackfillHandler extends HandlerCommon {

    public AlterTableGroupBackfillHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        AlterTableGroupBackfill backfill = (AlterTableGroupBackfill) logicalPlan;
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

        PhyTableOperationUtil.disableIntraGroupParallelism(schemaName, executionContext);

        Map<String, Set<String>> sourcePhyTables = backfill.getSourcePhyTables();
        Map<String, Set<String>> targetPhyTables = backfill.getTargetPhyTables();

        int affectRows = 0;
        if (!sourcePhyTables.isEmpty()) {
            affectRows = backfillExecutor
                .backfill(schemaName, logicalTable, executionContext, sourcePhyTables, targetPhyTables, backfill.getMovePartitions());
        }

        // Check target table immediately after backfill by default.
        assert !targetPhyTables.isEmpty();
        final boolean check =
            executionContext.getParamManager().getBoolean(ConnectionParams.TABLEGROUP_REORG_CHECK_AFTER_BACKFILL);
        if (check) {
            final boolean useFastChecker =
                FastChecker.isSupported(schemaName) &&
                    executionContext.getParamManager()
                        .getBoolean(ConnectionParams.TABLEGROUP_REORG_BACKFILL_USE_FASTCHECKER);
            if (useFastChecker && fastCheckWithCatchEx(backfill, executionContext)) {
                return new AffectRowCursor(affectRows);
            } else {
                checkInCN(backfill, executionContext);
            }
        }

        return new AffectRowCursor(affectRows);
    }

    protected boolean fastCheckWithCatchEx(AlterTableGroupBackfill backfill, ExecutionContext executionContext) {
        boolean fastCheckSucc = false;
        try {
            if(!backfill.getBroadcast()) {
                //if is not broadcast table, we execute fastcheck normally.
                fastCheckSucc = fastCheck(executionContext, backfill.getSchemaName(), backfill.getLogicalTableName(), backfill.getSourcePhyTables(), backfill.getTargetPhyTables());
            } else {
                /**
                 * FastChecker only allows checking one logic table each time.
                 * In broadcast case, the argument "targetPhyTables" in backfill contains many logical broadcast table, so we need to iterate each target logic table.
                 * */
                Map<String, Set<String>> srcPhyDbAndTables = backfill.getSourcePhyTables();
                int succeedCnt = 0;
                for(Map.Entry<String, Set<String>> entry : backfill.getTargetPhyTables().entrySet()) {
                    Map<String, Set<String>> targetPhyTables = ImmutableMap.of(entry.getKey(), entry.getValue());
                    if(!fastCheck(executionContext, backfill.getSchemaName(), backfill.getLogicalTableName(), srcPhyDbAndTables, targetPhyTables)) {
                        break;
                    } else {
                        succeedCnt++;
                    }
                }
                fastCheckSucc = (succeedCnt == backfill.getTargetPhyTables().size());
            }
        } catch (Throwable ex) {
            fastCheckSucc = false;
            String msg = String.format(
                    "Failed to use fastChecker to check alter tablegroup backFill because of throwing exceptions,  so use old checker instead");
            SQLRecorderLogger.ddlLogger.warn(msg, ex);
        }
        return fastCheckSucc;
    }

    boolean fastCheck(ExecutionContext executionContext,
                      String schemaName, String logicalTable, Map<String, Set<String>> srcPhyDbAndTables,
                      Map<String, Set<String>> dstPhyDbAndTables) {
        long startTime = System.currentTimeMillis();

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
            "FastChecker for alter tablegroup, schema [{0}] logical table [{1}] start",
            schemaName, logicalTable));
        final int fastCheckerParallelism =
            executionContext.getParamManager().getInt(ConnectionParams.TABLEGROUP_REORG_FASTCHECKER_PARALLELISM);

        FastChecker fastChecker = AlterTableGroupFastChecker
            .create(schemaName, logicalTable, srcPhyDbAndTables,
                dstPhyDbAndTables, fastCheckerParallelism, executionContext);
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
                            "alter tablegroup fastchecker retry exceed max times", e);
                    }
                } else {
                    //other exception, we simply throw out
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, e,
                        "alter tablegroup fastchecker failed to check");
                }
            } finally {
                tryTimes += 1;
                SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                    "FastChecker for alter tablegroup, schema [{0}] logical src table [{1}] finish, time use [{2}], check result [{3}]",
                    schemaName, logicalTable,
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

    private void checkInCN(AlterTableGroupBackfill backfill, ExecutionContext executionContext) {
        final long batchSize =
            executionContext.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_CHECK_BATCH_SIZE);
        final long speedLimit =
            executionContext.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_CHECK_SPEED_LIMITATION);
        final long speedMin =
            executionContext.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_CHECK_SPEED_MIN);
        final long parallelism =
            executionContext.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_CHECK_PARALLELISM);
        final long earlyFailNumber =
            executionContext.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_EARLY_FAIL_NUMBER);

        String schemaName = backfill.getSchemaName();
        String logicalTable = backfill.getLogicalTableName();
        Map<String, Set<String>> sourcePhyTables = backfill.getSourcePhyTables();
        Map<String, Set<String>> targetPhyTables = backfill.getTargetPhyTables();

        Checker checker = AlterTableGroupChecker.create(schemaName,
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
            targetPhyTables);
        checker.setInBackfill(true);

        if (null == executionContext.getDdlJobId() || 0 == executionContext.getDdlJobId()) {
            checker.setJobId(JOB_ID_GENERATOR.nextId());
        } else {
            checker.setJobId(executionContext.getDdlJobId());
        }

        // Run the simple check.
        final Reporter reporter = new AlterTableGroupReporter(earlyFailNumber);
        try {
            checker.check(executionContext, reporter);
        } catch (TddlNestableRuntimeException e) {
            if (e.getMessage().contains("Too many conflicts")) {
                throw GeneralUtil
                    .nestedException(
                        "alter tableGroup checker error limit exceeded. Please try to rollback/recover this job");
            } else {
                throw e;
            }
        }

        final List<CheckerManager.CheckerReport> checkerReports = reporter.getCheckerReports();
        if (!checkerReports.isEmpty()) {
            // Some error found.
            throw GeneralUtil.nestedException(
                "alter tableGroup checker found error after backfill. Please try to rollback/recover this job");
        }
    }
}
