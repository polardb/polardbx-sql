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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.corrector.Reporter;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.partitionmanagement.BackfillExecutor;
import com.alibaba.polardbx.executor.partitionmanagement.corrector.AlterTableGroupChecker;
import com.alibaba.polardbx.executor.partitionmanagement.corrector.AlterTableGroupReporter;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.AlterTableGroupBackfill;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        Map<String, Set<String>> sourcePhyTables = backfill.getSourcePhyTables();
        Map<String, Set<String>> targetPhyTables = backfill.getTargetPhyTables();

        int affectRows = 0;
        if (!sourcePhyTables.isEmpty()) {
            affectRows = backfillExecutor
                .backfill(schemaName, logicalTable, executionContext, sourcePhyTables);
        }

        // Check target table immediately after backfill by default.
        assert !targetPhyTables.isEmpty();
        final boolean check =
            executionContext.getParamManager().getBoolean(ConnectionParams.SCALEOUT_CHECK_AFTER_BACKFILL);
        if (check) {
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

        return new AffectRowCursor(affectRows);
    }
}
