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

package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.ClassifyResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.DuplicateCheckResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RowClassifier;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author chenmo.cm
 */
public class ReplaceRelocateWriter extends RelocateWriter {
    private final LogicalInsert parent;
    private final boolean containsAllUk;

    public ReplaceRelocateWriter(LogicalInsert parent,
                                 RelOptTable targetTable,
                                 DistinctWriter deleteWriter,
                                 DistinctWriter insertWriter,
                                 DistinctWriter modifyWriter,
                                 Mapping skTargetMapping,
                                 Mapping skSourceMapping,
                                 List<ColumnMeta> skMetas,
                                 boolean containsAllUk,
                                 boolean usePartFieldChecker) {
        super(targetTable, deleteWriter, insertWriter, modifyWriter, skTargetMapping, skSourceMapping, skMetas, false,
            usePartFieldChecker);
        this.parent = parent;
        this.containsAllUk = containsAllUk;
    }

    @Override
    public ClassifyResult classify(BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> identicalSk,
                                   SourceRows sourceRows, ExecutionContext ec, ClassifyResult result) {

        final List<DuplicateCheckResult> classifiedRows = sourceRows.valueRows;

        classifiedRows.forEach(row -> {
            final List<Object> before = row.before;
            final Map<Integer, ParameterContext> after = row.insertParam;

            final RelOptTable targetTable = getTargetTable();
            assert targetTable.getQualifiedName().size() == 2;
            final String schemaName = targetTable.getQualifiedName().get(0);
            final String tableName = targetTable.getQualifiedName().get(1);
            final TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);

            final boolean canWriteForScaleout = ComplexTaskPlanUtils.canWrite(tableMeta);
            final boolean isReadyToPublishForScaleout = ComplexTaskPlanUtils.isReadyToPublish(tableMeta);
            // Use delete + insert to avoid dup key error while adding column
            final boolean isOnlineModifyColumn = TableColumnUtils.isModifying(schemaName, tableName, ec);
            // If this table contains all local/global uk and sk does not modified, do REPLACE
            final boolean pushReplace =
                row.duplicated && row.doReplace && canPushReplace(ec) && identicalSk.test(this, Pair.of(before, after))
                    && (!canWriteForScaleout || isReadyToPublishForScaleout) && !isOnlineModifyColumn;
            addResult(before, after, row.duplicated, pushReplace, row.doInsert, ec, result);
        });

        return result;
    }

    protected void addResult(List<Object> before, Map<Integer, ParameterContext> after, boolean duplicated,
                             boolean pushReplace, boolean doInsert, ExecutionContext ec, ClassifyResult result) {
        if (!duplicated) {
            // INSERT
            result.insertRows.add(after);
        } else if (pushReplace) {
            // REPLACE
            result.replaceRows.add(after);
        } else {
            // DELETE + INSERT
            result.deleteRows.add(before);
            if (doInsert) {
                result.insertRows.add(after);
            }
        }
    }

    @Override
    public SourceRows getInput(ExecutionContext ec, ExecutionContext noUse,
                               Function<DistinctWriter, SourceRows> rowGenerator,
                               RowClassifier classifier, List<RelNode> outDeletePlans,
                               List<RelNode> outInsertPlans, List<RelNode> outModifyPlans,
                               List<RelNode> replicateOutDeletePlans, List<RelNode> replicateOutInsertPlans,
                               List<RelNode> replicateOutModifyPlans) {
        final SourceRows duplicatedRows = rowGenerator.apply(getDeleteWriter());

        final ClassifyResult classified = classifier.apply(this, duplicatedRows, new ClassifyResult());

        final List<List<Object>> deleteRows = classified.deleteRows;
        final List<Map<Integer, ParameterContext>> insertRows = classified.insertRows;
        final List<Map<Integer, ParameterContext>> replaceRows = classified.replaceRows;

        if (!replaceRows.isEmpty()) {
            final ExecutionContext replaceEc = ec.copy(new Parameters(replaceRows));

            final InsertWriter replaceWriter = getModifyWriter().unwrap(InsertWriter.class);

            List<RelNode> inputs = replaceWriter.getInput(replaceEc);
            outModifyPlans.addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));
            replicateOutModifyPlans
                .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList()));
        }

        if (!deleteRows.isEmpty()) {
            List<RelNode> inputs = getDeleteWriter().getInput(ec, (w) -> deleteRows);
            outDeletePlans.addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));
            replicateOutDeletePlans
                .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList()));
        }

        if (!insertRows.isEmpty()) {
            final ExecutionContext insertEc = ec.copy();
            insertEc.setParams(new Parameters(insertRows));

            final InsertWriter insertWriter = getInsertWriter().unwrap(InsertWriter.class);

            List<RelNode> inputs = insertWriter.getInput(insertEc);
            outInsertPlans.addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));
            replicateOutInsertPlans
                .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList()));
        }

        return duplicatedRows;
    }

    public boolean canPushReplace(ExecutionContext ec) {
        return isContainsAllUk() && checkIsolationLevel(ec);
    }

    public boolean checkIsolationLevel(ExecutionContext ec) {
        // We should not push down REPLACE if current isolation level is below RR, otherwise we may REPLACE more
        // conflict rows than expected, because SELECT FOR UPDATE that we previously used to select conflict rows
        // will not lock gap in isolation level below RR.
        return ec.getParamManager().getBoolean(ConnectionParams.DML_FORCE_PUSHDOWN_RC_REPLACE)
            || ec.getTxIsolation() > Connection.TRANSACTION_READ_COMMITTED;
    }

    public boolean isContainsAllUk() {
        return containsAllUk;
    }
}
