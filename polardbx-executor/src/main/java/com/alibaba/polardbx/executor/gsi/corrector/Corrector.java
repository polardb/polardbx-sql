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

package com.alibaba.polardbx.executor.gsi.corrector;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.corrector.CheckerCallback;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.InsertIndexExecutor;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCheckGsi;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class Corrector implements CheckerCallback {

    private static final Logger logger = LoggerFactory.getLogger(Corrector.class);

    private final AtomicLong primaryCounter = new AtomicLong(
        0);
    private final AtomicLong gsiCounter = new AtomicLong(
        0);
    private final LogicalCheckGsi.CorrectionType correctionType;

    private final SqlInsert replacePlan;
    private final PhyTableOperation deletePlan;
    private final BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc;

    private final ITransactionManager tm;

    public Corrector(String schemaName, LogicalCheckGsi.CorrectionType correctionType, SqlInsert replacePlan,
                     PhyTableOperation deletePlan,
                     BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc) {
        this.correctionType = correctionType;
        this.replacePlan = replacePlan;
        this.deletePlan = deletePlan;
        this.executeFunc = executeFunc;
        this.tm = ExecutorContext.getContext(schemaName).getTransactionManager();
    }

    public static Corrector create(Checker checker, LogicalCheckGsi.CorrectionType correctionType, ExecutionContext ec,
                                   BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc) {
        // Pre-check.
        if (correctionType != LogicalCheckGsi.CorrectionType.BASED_ON_PRIMARY) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER,
                "Only support correction based on primary.");
        }

        // Check lock mode.
        switch (correctionType) {
        case BASED_ON_PRIMARY:
            if (SqlSelect.LockMode.UNDEF == checker.getPrimaryLock()
                || checker.getGsiLock() != SqlSelect.LockMode.EXCLUSIVE_LOCK) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER,
                    "GSI corrector with incorrect row lock.");
            }
            break;

        case ADD_ONLY:
            if (SqlSelect.LockMode.UNDEF == checker.getPrimaryLock()
                || SqlSelect.LockMode.UNDEF == checker.getGsiLock()) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER,
                    "GSI corrector with incorrect row lock.");
            }
            break;

        case DROP_ONLY:
            if (checker.getPrimaryLock() != SqlSelect.LockMode.EXCLUSIVE_LOCK
                || checker.getGsiLock() != SqlSelect.LockMode.EXCLUSIVE_LOCK) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER,
                    "GSI corrector with incorrect row lock.");
            }
            break;

        default:
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER,
                "GSI corrector with unknown correction type.");
        }

        // Construct target table
        final SqlNode targetTableParam = BuildPlanUtils.buildTargetTable();

        // Construct targetColumnList
        final SqlNodeList targetColumnList = new SqlNodeList(
            checker.getIndexColumns()
                .stream()
                .map(col -> new SqlIdentifier(col, SqlParserPos.ZERO))
                .collect(Collectors.toList()),
            SqlParserPos.ZERO);

        // Construct values
        final SqlNode[] dynamics = new SqlNode[targetColumnList.size()];
        for (int i = 0; i < targetColumnList.size(); i++) {
            dynamics[i] = new SqlDynamicParam(i, SqlParserPos.ZERO);
        }
        final SqlNode row = new SqlBasicCall(SqlStdOperatorTable.ROW, dynamics, SqlParserPos.ZERO);
        final SqlNode[] rowList = new SqlNode[] {row};
        final SqlNode values = new SqlBasicCall(SqlStdOperatorTable.VALUES, rowList, SqlParserPos.ZERO);

        final SqlInsert replacePlan = SqlInsert.create(SqlKind.REPLACE,
            SqlParserPos.ZERO,
            new SqlNodeList(SqlParserPos.ZERO),
            targetTableParam,
            values,
            targetColumnList,
            SqlNodeList.EMPTY,
            0,
            null);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(checker.getSchemaName(), ec);
        final List<String> pks = checker.getPrimaryKeys()
            .stream()
            .mapToObj(idx -> checker.getIndexColumns().get(idx))
            .collect(Collectors.toList());
        final PhyTableOperation deletePlan = builder.buildDeleteForCorrector(pks);

        return new Corrector(checker.getSchemaName(), correctionType, replacePlan, deletePlan, executeFunc);
    }

    @Override
    public AtomicLong getPrimaryCounter() {
        return primaryCounter;
    }

    @Override
    public AtomicLong getGsiCounter() {
        return gsiCounter;
    }

    @Override
    public void start(ExecutionContext baseEc, Checker checker) {
        // Insert start mark.
        checker.getManager()
            .insertReports(ImmutableList.of(CheckerManager.CheckerReport
                .create(checker, "", "", "START", CheckerManager.CheckerReportStatus.START, "--", "--", "Corrector.")));
    }

    @Override
    public boolean batch(String dbIndex, String phyTable, ExecutionContext selectEc, Checker checker,
                         boolean primaryToGsi, List<List<Pair<ParameterContext, byte[]>>> baseRows,
                         List<List<Pair<ParameterContext, byte[]>>> checkRows) {

        final List<CheckerManager.CheckerReport> tmpResult = new ArrayList<>();
        final List<List<ParameterContext>> replaceRows = new ArrayList<>();
        final AtomicInteger mayAffectRows = new AtomicInteger(0);
        final List<List<ParameterContext>> dropRows = new ArrayList<>();
        final List<List<Pair<ParameterContext, byte[]>>> badShardRows = new ArrayList<>();

        // Check bad shard.
        for (List<Pair<ParameterContext, byte[]>> row : baseRows) {
            if (!checker.checkShard(dbIndex, phyTable, selectEc, primaryToGsi, row)) {
                badShardRows.add(row);
            }
        }

        // Check diff.
        checker.compareRows(baseRows, checkRows, (baseRow, checkRow) -> {
            assert baseRow != null; // Checker select pk IN (baseRows' pk), so baseRow is always valid.
            final boolean isBadShard = Collections.binarySearch(badShardRows, baseRow, checker.getRowComparator()) >= 0;

            if (isBadShard) {
                // Special bad case, just record it.
                tmpResult.add(CheckerManager.CheckerReport.create(checker,
                    dbIndex,
                    phyTable,
                    "ERROR_SHARD",
                    CheckerManager.CheckerReportStatus.FOUND,
                    GsiReporter.pkString(checker, baseRow),
                    GsiReporter
                        .detailString(checker, primaryToGsi ? baseRow : checkRow, primaryToGsi ? checkRow : baseRow),
                    "Do nothing."));

                return true;
            }

            if (null == checkRow) {
                final String extra;
                // Missing check.
                boolean repaired = false;
                if (primaryToGsi) {
                    // Missing GSI.
                    switch (correctionType) {
                    case BASED_ON_PRIMARY:
                    case ADD_ONLY:
                        // Add GSI.
                        replaceRows.add(baseRow.stream().map(Pair::getKey).collect(Collectors.toList()));
                        mayAffectRows.getAndAdd(1);
                        repaired = true;
                        extra = "Add to GSI.";
                        break;

                    case DROP_ONLY:
                        // Drop primary.
                        dropRows.add(baseRow.stream().map(Pair::getKey).collect(Collectors.toList()));
                        repaired = true;
                        extra = "Drop primary.";
                        break;

                    default:
                        throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER,
                            "Unknown corrector type: " + correctionType.getLabel());
                    }
                } else {
                    // Missing primary(orphan GSI).
                    switch (correctionType) {
                    case ADD_ONLY:
                        // Cannot add primary based on GSI.
                        extra = "Do nothing.";
                        break;

                    case BASED_ON_PRIMARY:
                    case DROP_ONLY:
                        // Drop GSI.
                        dropRows.add(baseRow.stream().map(Pair::getKey).collect(Collectors.toList()));
                        repaired = true;
                        extra = "Drop GSI.";
                        break;

                    default:
                        throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER,
                            "Unknown corrector type: " + correctionType.getLabel());
                    }
                }

                // Record to result.
                tmpResult.add(CheckerManager.CheckerReport.create(checker,
                    dbIndex,
                    phyTable,
                    primaryToGsi ? "MISSING" : "ORPHAN",
                    repaired ? CheckerManager.CheckerReportStatus.REPAIRED : CheckerManager.CheckerReportStatus.FOUND,
                    GsiReporter.pkString(checker, baseRow),
                    GsiReporter.detailString(checker, primaryToGsi ? baseRow : null, primaryToGsi ? null : baseRow),
                    extra));
            } else {
                // Conflict.
                if (primaryToGsi) {
                    final String extra;
                    // Only dealing here, because conflict rows appeal twice when reverse scan.
                    boolean repaired = false;
                    switch (correctionType) {
                    case BASED_ON_PRIMARY:
                        // Replace GSI.
                        replaceRows.add(baseRow.stream().map(Pair::getKey).collect(Collectors.toList()));
                        mayAffectRows.getAndAdd(2);
                        repaired = true;
                        extra = "Replace GSI.";
                        break;

                    case ADD_ONLY:
                    case DROP_ONLY:
                        // Do nothing.
                        extra = "Do nothing.";
                        break;

                    default:
                        throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER,
                            "Unknown corrector type: " + correctionType.getLabel());
                    }

                    // Record to result.
                    tmpResult.add(CheckerManager.CheckerReport.create(checker,
                        dbIndex,
                        phyTable,
                        "CONFLICT",
                        repaired ? CheckerManager.CheckerReportStatus.REPAIRED :
                            CheckerManager.CheckerReportStatus.FOUND,
                        GsiReporter.pkString(checker, baseRow),
                        GsiReporter.detailString(checker, baseRow, checkRow),
                        extra));
                }
            }

            return true;
        });

        // Now repair it in XA.
        final boolean fixResult = GsiUtils.wrapWithDistributedTrx(tm, selectEc, correctorEc -> {
            if (!replaceRows.isEmpty()) {
                // Replace(insert or delete then insert) first.
                final List<Map<Integer, ParameterContext>> batchParams = replaceRows.stream().map(row -> {
                    final Map<Integer, ParameterContext> param = new HashMap<>();
                    for (int i = 0; i < row.size(); ++i) {
                        param.put(i + 1, row.get(i));
                    }
                    return param;
                }).collect(Collectors.toList());

                int affectRows = applyBatchReplace(checker.getSchemaName(),
                    primaryToGsi ? checker.getIndexName() : checker.getTableName(),
                    batchParams,
                    correctorEc.copy());

                if (affectRows != mayAffectRows.get()) {
                    selectEc.getTransaction().rollback();
                    correctorEc.getTransaction().rollback();
                    SQLRecorderLogger.ddlLogger
                        .error(MessageFormat.format("GSI corrector replace mismatch affect rows. expected:{0} real:{1}",
                            mayAffectRows.get(),
                            affectRows));
                    return false;
                }
            }

            if (!dropRows.isEmpty()) {
                // Drop extra rows.
                for (List<ParameterContext> row : dropRows) {
                    final List<ParameterContext> pks = checker.getPrimaryKeys()
                        .stream()
                        .mapToObj(row::get)
                        .collect(Collectors.toList());
                    // Delete always on base row(base table).
                    int affectRows = doDelete(dbIndex, phyTable, pks, selectEc.copy());

                    if (affectRows != 1) {
                        // Fail to delete? Retry.
                        selectEc.getTransaction().rollback();
                        correctorEc.getTransaction().rollback();
                        SQLRecorderLogger.ddlLogger.error(MessageFormat
                            .format("GSI corrector delete mismatch affect rows. expected:{0} real:{1}", 1, affectRows));
                        return false;
                    }
                }
            }

            // All done, commit selectEc and then commit XA write.
            try {
                selectEc.getTransaction().commit();
                correctorEc.getTransaction().commit();
            } catch (Exception e) {
                logger.error("Close extract statement or commit correction XA write failed!", e);
                correctorEc.getTransaction().rollback();
                return false;
            }

            // This batch is done.
            return true;
        });

        if (fixResult) {
            // Write tmp result to checker report table.
            checker.getManager().insertReports(tmpResult);
        }

        return fixResult; // Retry this batch if needed.
    }

    @Override
    public void finish(ExecutionContext baseEc, Checker checker) {
        // Insert finish mark.
        final String finishDetails = "" + primaryCounter + '/' + gsiCounter + " rows checked.";
        checker.getManager()
            .insertReports(ImmutableList.of(CheckerManager.CheckerReport.create(checker,
                "",
                "",
                "SUMMARY",
                CheckerManager.CheckerReportStatus.FINISH,
                "--",
                finishDetails,
                "Corrector.")));
    }

    private int doDelete(String dbIndex, String phyTable, List<ParameterContext> pks, ExecutionContext newEc) {
        final Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        // Get Plan
        final PhyTableOperation plan = new PhyTableOperation(deletePlan);

        int nextParamIndex = 2;

        // Parameters for where (pks) = (values)
        for (int i = 0; i < pks.size(); ++i) {
            planParams.put(nextParamIndex,
                new ParameterContext(pks.get(i).getParameterMethod(),
                    new Object[] {nextParamIndex, pks.get(i).getValue()}));
            ++nextParamIndex;
        }

        plan.setDbIndex(dbIndex);
        plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
        plan.setParam(planParams);

        return applyDelete(plan, planParams, newEc);
    }

    private int applyBatchReplace(String schemaName, String tableName, List<Map<Integer, ParameterContext>> batchParams,
                                  ExecutionContext newEc) {
        // Construct params for each batch
        Parameters parameters = new Parameters();
        parameters.setBatchParams(batchParams);

        newEc.setParams(parameters);

        return executeInsert(replacePlan, schemaName, tableName, newEc);
    }

    private int executeInsert(SqlInsert sqlInsert, String schemaName, String tableName,
                              ExecutionContext executionContext) {
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        return InsertIndexExecutor
            .insertIntoTable(null, sqlInsert, tableMeta, schemaName, executionContext, executeFunc, false);
    }

    private int applyDelete(PhyTableOperation deleteOp, Map<Integer, ParameterContext> params, ExecutionContext newEc) {
        Parameters parameters = new Parameters();
        parameters.setParams(params);

        newEc.setParams(parameters);

        List<Cursor> cursors = executeFunc.apply(ImmutableList.of(deleteOp), newEc);
        return ExecUtils.getAffectRowsByCursors(cursors, false);
    }
}
