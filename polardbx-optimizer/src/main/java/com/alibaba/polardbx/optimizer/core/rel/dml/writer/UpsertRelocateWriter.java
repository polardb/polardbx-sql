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
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.ClassifyResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.DuplicateCheckResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RowClassifier;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;

import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author chenmo.cm
 */
public class UpsertRelocateWriter extends RelocateWriter {
    private final LogicalInsert parent;

    /**
     * Insert writer for rows not duplicated
     */
    private final InsertWriter simpleInsertWriter;

    /**
     * Insert writer for INSERT then UPDATE rows
     */
    private final InsertWriter insertThenUpdateWriter;

    public UpsertRelocateWriter(LogicalInsert parent, RelOptTable targetTable,
                                InsertWriter simpleInsertWriter,
                                InsertWriter insertThenUpdateWriter,
                                DistinctWriter relocateDeleteWriter,
                                DistinctWriter relocateInsertWriter,
                                DistinctWriter modifyWriter,
                                Mapping skTargetMapping,
                                Mapping skSourceMapping,
                                List<ColumnMeta> skMetas,
                                boolean modifySkOnly) {
        super(targetTable, relocateDeleteWriter, relocateInsertWriter, modifyWriter, skTargetMapping, skSourceMapping,
            skMetas, modifySkOnly);
        this.parent = parent;
        this.simpleInsertWriter = simpleInsertWriter;
        this.insertThenUpdateWriter = insertThenUpdateWriter;
    }

    @Override
    public ClassifyResult classify(BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> identicalSk,
                                   SourceRows sourceRows, ExecutionContext ec, ClassifyResult result) {
        final List<DuplicateCheckResult> classifiedRows = sourceRows.valueRows;

        classifiedRows.stream().filter(r -> !r.skipUpdate()).forEach(row -> {
            final boolean insertThenUpdate = row.insertThenUpdate();
            final boolean updateOnly = row.updateOnly();

            // If partition key is not modified do UPDATE, or else do DELETE + INSERT
            final boolean doUpdate = updateOnly && identicalSk.test(this, Pair.of(row.updateSource, null));

            addResult(row.before, row.after, row.updateSource, row.insertParam, row.duplicated, insertThenUpdate,
                doUpdate, ec, result);
        });

        return result;
    }

    /**
     * Bind insert row to operation
     *
     * @param before Original values of selected row
     * @param after Updated values of selected row
     * @param merged For update use, concat before and after value in one row
     * @param insertParam Parameter row for INSERT new row
     * @param duplicated True if insert row is duplicated with selected row
     * @param insertThenUpdate Insert then update
     */
    protected void addResult(List<Object> before, List<Object> after, List<Object> merged,
                             Map<Integer, ParameterContext> insertParam, boolean duplicated, boolean insertThenUpdate,
                             boolean doUpdate, ExecutionContext ec, ClassifyResult result) {
        if (!duplicated) {
            // INSERT
            result.insertRows.add(insertParam);
        } else {
            if (insertThenUpdate) {
                // INSERT
                result.insertThenUpdateRows.add(after);
            } else if (doUpdate) {
                // UPDATE
                result.updateBeforeRows.add(before);
                result.updateAfterRows.add(merged);
            } else {
                // DELETE + INSERT
                result.relocateBeforeRows.add(before);
                result.relocateAfterRows.add(after);
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

        // INSERT rows
        final List<Map<Integer, ParameterContext>> insertRows = classified.insertRows;
        final List<List<Object>> insertThenUpdateRows = classified.insertThenUpdateRows;
        // UPDATE rows
        final List<List<Object>> updateBeforeRow = classified.updateBeforeRows;
        final List<List<Object>> updateAfterRows = classified.updateAfterRows;
        // DELETE + INSERT rows
        final List<List<Object>> relocateBeforeRows = classified.relocateBeforeRows;
        final List<List<Object>> relocateAfterRows = classified.relocateAfterRows;

        if (!updateAfterRows.isEmpty()) {
            final ExecutionContext updateEc = ec.copy(new Parameters());

            final DistinctWriter updateWriter = getModifyWriter();

            List<RelNode> inputs = updateWriter.getInput(updateEc, (w) -> updateAfterRows);
            outModifyPlans.addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));
            replicateOutModifyPlans
                .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList()));

        }

        if (!relocateBeforeRows.isEmpty()) {
            List<RelNode> inputs = getDeleteWriter().getInput(ec, (w) -> relocateBeforeRows);
            outDeletePlans.addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));
            replicateOutDeletePlans
                .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList()));
        }

        if (!relocateAfterRows.isEmpty()) {
            final List<Map<Integer, ParameterContext>> params = BuildPlanUtils.buildInsertBatchParam(relocateAfterRows);
            final ExecutionContext insertEc = ec.copy(new Parameters(params));

            final InsertWriter insertWriter = getInsertWriter().unwrap(InsertWriter.class);

            List<RelNode> inputs = insertWriter.getInput(insertEc);
            outInsertPlans.addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));
            replicateOutInsertPlans
                .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList()));
        }

        if (!insertRows.isEmpty()) {
            final ExecutionContext insertEc = ec.copy(new Parameters(insertRows));

            final InsertWriter insertWriter = getSimpleInsertWriter().unwrap(InsertWriter.class);

            List<RelNode> inputs = insertWriter.getInput(insertEc);
            outInsertPlans.addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));
            replicateOutInsertPlans
                .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList()));
        }

        if (!insertThenUpdateRows.isEmpty()) {
            final List<Map<Integer, ParameterContext>> params =
                BuildPlanUtils.buildInsertBatchParam(insertThenUpdateRows);
            final ExecutionContext insertEc = ec.copy(new Parameters(params));

            final InsertWriter insertWriter = getInsertThenUpdateWriter();

            List<RelNode> inputs = insertWriter.getInput(insertEc);
            outInsertPlans.addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));
            replicateOutInsertPlans
                .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList()));
        }

        return duplicatedRows;
    }

    public InsertWriter getSimpleInsertWriter() {
        return simpleInsertWriter;
    }

    public InsertWriter getInsertThenUpdateWriter() {
        return insertThenUpdateWriter;
    }

    @Override
    public List<Writer> getInputs() {
        final List<Writer> writerList = Lists.newArrayList();
        writerList.add(simpleInsertWriter);
        writerList.add(insertThenUpdateWriter);
        writerList.addAll(super.getInputs());
        return writerList;
    }

}
