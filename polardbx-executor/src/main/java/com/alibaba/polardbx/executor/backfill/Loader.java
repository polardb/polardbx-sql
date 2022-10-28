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

package com.alibaba.polardbx.executor.backfill;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.rel.PhyOperationBuilderCommon;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlInsert;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static com.alibaba.polardbx.ErrorCode.ER_DUP_ENTRY;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DUP_ENTRY;

/**
 * Fill batch data into index table with duplication check
 */
public abstract class Loader extends PhyOperationBuilderCommon {

    private final String schemaName;
    private final String tableName;
    private final SqlInsert sqlInsert;
    private final SqlInsert sqlInsertIgnore;
    /**
     * <pre>
     * SELECT pk0, ... , pkn, sk_primary_0, ... , sk_primary_n, sk_index_0, ... , sk_index_n
     * FROM {logical_index_table}
     * WHERE pk0 <=> ? AND ... AND pkn <=> ?
     *   AND sk_primary_0 <=> ? AND ... AND sk_primary_n <=> ?
     *   AND sk_index_0 <=> ? AND ... AND sk_index_n <=> ?
     * LIMIT 1
     * </pre>
     */
    private final ExecutionPlan checkerPlan;
    private final int[] checkerParamMapping;
    private final int[] checkerPkMapping;
    private final ITransactionManager tm;
    protected final boolean mirrorCopy;

    protected final BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc;

    protected Loader(String schemaName, String tableName, SqlInsert insert, SqlInsert insertIgnore,
                     ExecutionPlan checkerPlan, int[] checkerPkMapping, int[] checkerParamMapping,
                     BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc, boolean mirrorCopy) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.sqlInsert = insert;
        this.sqlInsertIgnore = insertIgnore;
        this.checkerPlan = checkerPlan;
        this.checkerPkMapping = checkerPkMapping;
        this.checkerParamMapping = checkerParamMapping;
        this.executeFunc = executeFunc;
        this.tm = ExecutorContext.getContext(schemaName).getTransactionManager();
        this.mirrorCopy = mirrorCopy;
    }

    /**
     * Insert into index table
     */
    public int fillIntoIndex(List<Map<Integer, ParameterContext>> batchParams,
                             Pair<ExecutionContext, Pair<String,String>> baseEcAndIndexPair,
                             Supplier<Boolean> checker) {
        if (batchParams.isEmpty()) {
            return 0;
        }

        // Batch insert
        final Integer batchInsertResult =
            GsiUtils.wrapWithDistributedTrx(tm, baseEcAndIndexPair.getKey(), (insertEc) -> {
                int result = -1;
                try {
                    // Batch insert
                    result = applyBatch(batchParams, insertEc.copy(), baseEcAndIndexPair.getValue().getKey(), baseEcAndIndexPair.getValue().getValue());

                    // Batch insert success, check lock exists
                    return checkBeforeCommit(checker, insertEc, result);
                } catch (TddlNestableRuntimeException e) {
                    // Batch insert failed
                    SQLRecorderLogger.ddlLogger
                        .warn(MessageFormat.format("[{0}] Batch insert failed first row: {1} cause: {2}",
                            baseEcAndIndexPair.getKey().getTraceId(),
                            GsiUtils.rowToString(batchParams.get(0)),
                            e.getMessage()));

                    if (GsiUtils.vendorErrorIs(e, SQLSTATE_DUP_ENTRY, ER_DUP_ENTRY)) {
                        // Duplicated key exception
                        return -1;
                    } else {
                        throw e;
                    }
                }
            });

        if (batchInsertResult >= 0) {
            return batchInsertResult;
        }

        // Fall back to single insert
        return GsiUtils
            .wrapWithDistributedTrx(tm, baseEcAndIndexPair.getKey(), (insertEc) -> {
                int result = 0;

                for (Map<Integer, ParameterContext> param : batchParams) {
                    int single = applyRow(param, insertEc.copy(), baseEcAndIndexPair.getValue().getKey(), baseEcAndIndexPair.getValue().getValue(), true);

                    if (single < 1) {
                        // Compare row
                        checkDuplicate(param, insertEc.copy());

                        // If identical row found
                        single = 1;
                    }

                    result += single;
                }

                return checkBeforeCommit(checker, insertEc, result);
            });
    }

    /**
     * Select from index table with primary key, sharding key of primary table and
     * index table. Non-empty result means rows in batch and index table is
     * identical
     *
     * @param baseParam Parameters for insert
     * @param checkerEc ExecutionContext for select plan
     */
    private void checkDuplicate(Map<Integer, ParameterContext> baseParam, ExecutionContext checkerEc) {
        final List<String> pkParams = new ArrayList<>();
        final Map<Integer, ParameterContext> checkerParam = new HashMap<>();
        for (int i = 0; i < checkerParamMapping.length; i++) {
            final int filterIndex = checkerParamMapping[i] + 1;
            final ParameterContext old = baseParam.get(filterIndex);
            checkerParam.put(i + 1,
                new ParameterContext(old.getParameterMethod(), new Object[] {i + 1, old.getArgs()[1]}));
            if (i < checkerPkMapping.length) {
                final ParameterContext pkParam = baseParam.get(checkerPkMapping[i] + 1);
                pkParams.add(Optional.ofNullable(pkParam.getArgs()[1]).map(Object::toString).orElse("NULL"));
            }
        }
        final Parameters parameters = new Parameters();
        parameters.setParams(checkerParam);

        checkerEc.setParams(parameters);

        final List<Throwable> exList = new ArrayList<>();
        Cursor checkerCursor = null;
        try {
            // Check
            checkerCursor = ExecutorHelper.execute(checkerPlan.getPlan(), checkerEc);

            if (checkerCursor.next() != null) {
                // Duplicated row in index table has identical primary key, primary sharding key
                // and index sharding key with primary table, which means this row is identical
                // with primary table, just skip.
                return;
            }

            // No local unique index exists when doing backfill
            exList.add(new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_BACKFILL_DUPLICATE_ENTRY,
                String.join("-", pkParams),
                "PRIMARY"));
        } finally {
            if (null != checkerCursor) {
                checkerCursor.close(exList);
            }
        }

        if (!exList.isEmpty()) {
            throw GeneralUtil.nestedException(exList.get(0));
        }
    }

    private static Integer checkBeforeCommit(Supplier<Boolean> checker, ExecutionContext insertEc, int result) {
        if (checker.get()) {
            insertEc.getTransaction().commit();
            return result;
        } else {
            insertEc.getTransaction().rollback();
            // This means extractor's connection fail. Throw a special error.
            throw new TddlNestableRuntimeException(
                "Loader check error. Fail to commit in backfill extractor. Please retry or recover DDL later.");
        }
    }

    /**
     * Batch insert
     *
     * @param batchParams Batch parameters
     * @param newEc Copied ExecutionContext
     * @return Affected rows
     */
    private int applyBatch(List<Map<Integer, ParameterContext>> batchParams, ExecutionContext newEc,
                           String sourceDbIndex, String phyTableName) {
        // Construct params for each batch
        Parameters parameters = new Parameters();
        parameters.setBatchParams(batchParams);

        newEc.setParams(parameters);

        return executeInsert(sqlInsert, schemaName, tableName, newEc, sourceDbIndex, phyTableName);
    }

    /**
     * Single insert
     *
     * @param param Parameter
     * @param newEc Copied ExecutionContext
     * @param sourceDbIndex the rows extract from which physicalDb
     * @param ignore Use insert ignore
     * @return Affected rows
     */
    private int applyRow(Map<Integer, ParameterContext> param, ExecutionContext newEc, String sourceDbIndex,
                         String phyTableName,
                         boolean ignore) {
        Parameters parameters = new Parameters();
        parameters.setParams(param);

        newEc.setParams(parameters);

        return executeInsert(ignore ? sqlInsertIgnore : sqlInsert, schemaName, tableName, newEc, sourceDbIndex, phyTableName);
    }

    public abstract int executeInsert(SqlInsert sqlInsert, String schemaName, String tableName,
                                      ExecutionContext executionContext, String sourceDbIndex, String phyTableName);
}
