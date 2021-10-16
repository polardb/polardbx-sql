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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.CaseWhenWriter;
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
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Upsert which does not modify partition key
 *
 * @author chenmo.cm
 */
public class UpsertWriter extends AbstractSingleWriter implements CaseWhenWriter {
    private final LogicalInsert parent;

    private final InsertWriter insertWriter;
    private final DistinctWriter updateWriter;
    /**
     * Insert writer for INSERT then UPDATE rows
     */
    private final InsertWriter insertThenUpdateWriter;

    /**
     * Gsi table may not contains any column in ON DUPLICATE KEY UPDATE statement
     */
    private final boolean withoutUpdate;

    public UpsertWriter(LogicalInsert parent, RelOptTable targetTable, InsertWriter insertWriter,
                        InsertWriter insertThenUpdateWriter, DistinctWriter updateWriter, boolean withoutUpdate) {
        super(targetTable, Operation.INSERT);
        this.parent = parent;
        this.insertWriter = insertWriter;
        this.insertThenUpdateWriter = insertThenUpdateWriter;
        this.updateWriter = updateWriter;
        this.withoutUpdate = withoutUpdate;
    }

    public InsertWriter getInsertWriter() {
        return insertWriter;
    }

    public InsertWriter getInsertThenUpdateWriter() {
        return insertThenUpdateWriter;
    }

    public DistinctWriter getUpdaterWriter() {
        return updateWriter;
    }

    public SourceRows getInput(ExecutionContext ec, SourceRows duplicatedRows, RowClassifier classifier,
                               List<RelNode> outDeletePlans, List<RelNode> outInsertPlans,
                               List<RelNode> outModifyPlans, List<RelNode> replicateOutDeletePlans,
                               List<RelNode> replicateOutInsertPlans, List<RelNode> replicateOutModifyPlans) {
        final ClassifyResult classified = classifier.apply(this, duplicatedRows, new ClassifyResult());

        // INSERT rows
        final List<Map<Integer, ParameterContext>> insertRows = new ArrayList<>(classified.insertRows);
        final List<List<Object>> insertThenUpdateRows = classified.insertThenUpdateRows;
        // UPDATE rows
        final List<List<Object>> updateAfterRows = classified.updateAfterRows;

        if (!withoutUpdate && !updateAfterRows.isEmpty()) {
            final ExecutionContext updateEc = ec.copy(new Parameters());

            final DistinctWriter updateWriter = getUpdaterWriter();
            List<RelNode> inputs = updateWriter.getInput(updateEc, (w) -> updateAfterRows);
            outModifyPlans.addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));
            replicateOutModifyPlans
                .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList()));
        }

        if (!insertRows.isEmpty()) {
            final ExecutionContext insertEc = ec.copy(new Parameters(insertRows));

            final InsertWriter insertWriter = getInsertWriter().unwrap(InsertWriter.class);

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

    @Override
    public ClassifyResult classify(BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> comparator,
                                   SourceRows sourceRows, ExecutionContext ec, ClassifyResult result) {
        final List<DuplicateCheckResult> classifiedRows = sourceRows.valueRows;

        // Skip update that change nothing
        classifiedRows.stream().filter(r -> !r.skipUpdate()).forEach(
            row -> addResult(row.before, row.after, row.updateSource, row.insertParam, row.duplicated, row.doInsert, ec,
                result));

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
                             ExecutionContext ec, ClassifyResult result) {
        if (!duplicated) {
            // INSERT
            result.insertRows.add(insertParam);
        } else {
            if (insertThenUpdate) {
                // INSERT then UPDATE
                result.insertThenUpdateRows.add(after);
            } else {
                // UPDATE
                result.updateBeforeRows.add(before);
                result.updateAfterRows.add(merged);
            }
        }
    }

    @Override
    public List<Writer> getInputs() {
        final List<Writer> writerList = Lists.newArrayList();
        writerList.add(insertWriter);
        writerList.add(insertThenUpdateWriter);
        writerList.add(updateWriter);
        return writerList;
    }
}
