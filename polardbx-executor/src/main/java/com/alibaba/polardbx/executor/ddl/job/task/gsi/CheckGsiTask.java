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

package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.corrector.CheckerCallback;
import com.alibaba.polardbx.executor.corrector.Reporter;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.corrector.Corrector;
import com.alibaba.polardbx.executor.gsi.corrector.GsiChecker;
import com.alibaba.polardbx.executor.gsi.corrector.GsiReporter;
import com.alibaba.polardbx.executor.gsi.fastchecker.GsiFastChecker;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCheckGsi;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CheckGsiPrepareData;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
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
 * Check consistency of global index
 *
 * @author moyi
 * @since 2021/07
 */
@Getter
@TaskName(name = "CheckGsiTask")
public class CheckGsiTask extends BaseBackfillTask {

    private final static Logger LOG = LoggerFactory.getLogger(CheckGsiTask.class);

    private final String tableName;
    private final String indexName;
    private final String primaryTableLockMode;
    private final String indexTableLockMode;
    private final GsiChecker.Params checkParams;
    private final boolean correct;
    private final String extraCmd;
    private final boolean primaryBroadCast;
    private final boolean gsiBroadCast;

    public static CheckGsiTask create(CheckGsiPrepareData prepareData) {
        return new CheckGsiTask(
            prepareData.getSchemaName(),
            prepareData.getTableName(),
            prepareData.getIndexName(),
            prepareData.getLockMode().getKey().name(),
            prepareData.getLockMode().getValue().name(),
            new GsiChecker.Params(
                prepareData.getBatchSize(),
                prepareData.getSpeedLimit(),
                prepareData.getSpeedMin(),
                prepareData.getParallelism(),
                prepareData.getEarlyFailNumber()
            ),
            prepareData.isCorrect(),
            prepareData.getExtraCmd(),
            false,
            false
        );
    }

    @JSONCreator
    public CheckGsiTask(String schemaName,
                        String tableName,
                        String indexName,
                        String primaryTableLockMode,
                        String indexTableLockMode,
                        GsiChecker.Params checkParams,
                        boolean correct,
                        String extraCmd,
                        boolean primaryBroadCast,
                        boolean gsiBroadCast) {
        super(schemaName);
        this.tableName = tableName;
        this.indexName = indexName;
        this.primaryTableLockMode = primaryTableLockMode;
        this.indexTableLockMode = indexTableLockMode;
        this.checkParams = checkParams;
        this.correct = correct;
        this.extraCmd = extraCmd;
        this.primaryBroadCast = primaryBroadCast;
        this.gsiBroadCast = gsiBroadCast;
    }

    @Override
    protected void executeImpl(ExecutionContext ec) {
        ec = ec.copy();
        ec.setBackfillId(getTaskId());

        // fast checker
        if (isUseFastChecker(ec) && fastCheck(ec)) {
            return;
        }

        // slow checker
        Checker checker = buildChecker(ec);
        checker.setJobId(ec.getDdlJobId());
        CheckerCallback callback = buildCheckCallback(checker, ec);
        try {
            checker.check(ec, callback);
        } catch (Throwable t) {
            if (StringUtils.containsIgnoreCase(t.getMessage(), "Too many conflicts")) {
                //ignore
            } else {
                throw t;
            }
        }
    }

    private Checker buildChecker(ExecutionContext ec) {
        return GsiChecker.create(
            schemaName, tableName, indexName,
            this.checkParams,
            SqlSelect.LockMode.valueOf(primaryTableLockMode),
            SqlSelect.LockMode.valueOf(indexTableLockMode),
            ec);
    }

    protected boolean fastCheckWithCatchEx(ExecutionContext ec) {
        boolean fastCheckSucc = false;
        try {
            if (gsiBroadCast && !primaryBroadCast) {
                int succeedCnt = 0;
                Map<String, Set<String>> dstPhyDbAndTables = GsiUtils.getPhyTables(schemaName, indexName);

                for (Map.Entry<String, Set<String>> entry : dstPhyDbAndTables.entrySet()) {
                    Map<String, Set<String>> targetPhyTables = ImmutableMap.of(entry.getKey(), entry.getValue());
                    if (!fastCheck(ec, null, targetPhyTables)) {
                        break;
                    } else {
                        succeedCnt++;
                    }
                }
                fastCheckSucc = (succeedCnt == dstPhyDbAndTables.size());
            } else if (primaryBroadCast && !gsiBroadCast) {
                int succeedCnt = 0;
                Map<String, Set<String>> srcPhyDbAndTables = GsiUtils.getPhyTables(schemaName, tableName);

                for (Map.Entry<String, Set<String>> entry : srcPhyDbAndTables.entrySet()) {
                    Map<String, Set<String>> sourcePhyTables = ImmutableMap.of(entry.getKey(), entry.getValue());
                    if (!fastCheck(ec, sourcePhyTables, null)) {
                        break;
                    } else {
                        succeedCnt++;
                    }
                }
                fastCheckSucc = (succeedCnt == srcPhyDbAndTables.size());
            } else {
                fastCheckSucc = fastCheck(ec);
            }
        } catch (Throwable ex) {
            fastCheckSucc = false;
            String msg = String.format(
                "Failed to use fastChecker to check gsi backFill because of throwing exceptions,  so use old checker instead");
            SQLRecorderLogger.ddlLogger.warn(msg, ex);
            LOG.warn(msg, ex);
        }
        return fastCheckSucc;
    }

    // TODO(moyi) do not execute task directly
    public void checkInBackfill(ExecutionContext ec) {
        if (isUseFastChecker(ec) && fastCheck(ec)) {
            return;
        }

        Checker checker = buildChecker(ec);
        checker.setInBackfill(true);
        checker.setJobId(ec.getDdlJobId());
        Reporter reporter = new Reporter(checkParams.getEarlyFailNumber());
        ExecutionContext checkerEc = ec.copy();

        try {
            checker.check(checkerEc, reporter);
        } catch (TddlNestableRuntimeException e) {
            if (e.getMessage().contains("Too many conflicts")) {
                throw GeneralUtil.nestedException(
                    "GSI checker error limit exceeded. Please try to rollback/recover this job");
            } else {
                throw e;
            }
        }

        List<CheckerManager.CheckerReport> checkerReports = reporter.getCheckerReports();
        if (!checkerReports.isEmpty()) {
            for (CheckerManager.CheckerReport report : checkerReports) {
                LOG.error("report detail: " + report);
            }
            // Some error found.
            throw GeneralUtil.nestedException(
                "GSI checker found error when creating GSI. Please try to rollback/recover this job");
        }
    }

    private boolean isUseFastChecker(ExecutionContext ec) {
        return FastChecker.isSupported(schemaName) &&
            ec.getParamManager().getBoolean(ConnectionParams.GSI_BACKFILL_USE_FASTCHECKER);
    }

    private boolean fastCheck(ExecutionContext ec) {
        return fastCheck(ec, null, null);
    }

    private boolean fastCheck(ExecutionContext ec, Map<String, Set<String>> srcPhyDbAndTables,
                              Map<String, Set<String>> dstPhyDbAndTables) {
        long startTime = System.currentTimeMillis();
        SQLRecorderLogger.ddlLogger.warn(MessageFormat
            .format("FastChecker for GSI, schema [{0}] logical src table [{1}] logic dst table [{2}] start",
                schemaName, tableName, indexName));

        final int parallelism = ec.getParamManager().getInt(ConnectionParams.GSI_FASTCHECKER_PARALLELISM);
        final int maxRetryTimes = ec.getParamManager().getInt(ConnectionParams.FASTCHECKER_RETRY_TIMES);

        FastChecker fastChecker = GsiFastChecker.create(schemaName, tableName, indexName, parallelism, ec);
        if (dstPhyDbAndTables != null) {
            fastChecker.setDstPhyDbAndTables(dstPhyDbAndTables);
        }

        if (srcPhyDbAndTables != null) {
            fastChecker.setSrcPhyDbAndTables(srcPhyDbAndTables);
        }

        boolean fastCheckResult = false;

        int tryTimes = 0;
        while (tryTimes < maxRetryTimes && !fastCheckResult) {
            try {
                fastCheckResult = fastChecker.check(ec);
            } catch (TddlNestableRuntimeException e) {
                if (StringUtils.containsIgnoreCase(e.getMessage(), "acquire lock timeout")) {
                    //if acquire lock timeout, we will retry
                    if (tryTimes < maxRetryTimes - 1) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(2000L * (1 + tryTimes));
                        } catch (InterruptedException ex) {
                            throw new TddlNestableRuntimeException(ex);
                        }
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER,
                            "gsi fastchecker retry exceed max times", e);
                    }
                } else {
                    //other exception, we simply throw out
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER,
                        "gsi fastchecker failed to check", e);
                }
            } finally {
                tryTimes += 1;

                SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                    "FastChecker for GSI, schema [{0}] logical src table [{1}] logic dst table [{2}] finish, time use [{3}], check result [{4}]",
                    schemaName, tableName, indexName,
                    (System.currentTimeMillis() - startTime) / 1000.0,
                    fastCheckResult ? "pass" : "not pass")
                );
                if (!fastCheckResult) {
                    EventLogger.log(EventType.DDL_WARN, "FastChecker failed");
                }
            }
        }

        if (fastCheckResult) {
            fastChecker.reportCheckOk(ec);
            return true;
        }
        return false;
    }

    private CheckerCallback buildCheckCallback(Checker checker, ExecutionContext executionContext) {
        if (correct) {
            FailPoint.injectRandomExceptionFromHint(executionContext);
            FailPoint.injectRandomSuspendFromHint(executionContext);
            Corrector corrector = Corrector
                .create(checker, LogicalCheckGsi.CorrectionType.of(extraCmd), executionContext, (inputs, runEc) -> {
                    QueryConcurrencyPolicy queryConcurrencyPolicy = getQueryConcurrencyPolicy(runEc);
                    List<Cursor> inputCursors = new ArrayList<>(inputs.size());
                    executeWithConcurrentPolicy(runEc, inputs, queryConcurrencyPolicy, inputCursors, schemaName);
                    return inputCursors;
                });
            return corrector;
        } else {
            GsiReporter reporter = new GsiReporter(checkParams.getEarlyFailNumber());
            return reporter;
        }
    }

    @Override
    public String remark() {
        return String.format("|Check(%s.%s)", tableName, indexName);
    }
}
