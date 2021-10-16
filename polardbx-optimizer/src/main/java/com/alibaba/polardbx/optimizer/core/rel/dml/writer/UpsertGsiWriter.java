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
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.GsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.ClassifyResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RowClassifier;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author chenmo.cm
 */
public class UpsertGsiWriter extends UpsertWriter implements GsiWriter {
    private final TableMeta gsiMeta;
    private final DistinctWriter deleteWriter;

    public UpsertGsiWriter(LogicalInsert parent,
                           RelOptTable targetTable,
                           InsertWriter insertWriter,
                           InsertWriter insertThenUpdateWriter,
                           DistinctWriter updateWriter,
                           DistinctWriter deleteWriter,
                           boolean withoutUpdate,
                           TableMeta gsiMeta) {
        super(parent, targetTable, insertWriter, insertThenUpdateWriter, updateWriter, withoutUpdate);
        this.gsiMeta = gsiMeta;
        this.deleteWriter = deleteWriter;
    }

    @Override
    public SourceRows getInput(ExecutionContext ec, SourceRows duplicatedRows, RowClassifier classifier,
                               List<RelNode> outDeletePlans, List<RelNode> outInsertPlans,
                               List<RelNode> outModifyPlans, List<RelNode> replicateOutDeletePlans,
                               List<RelNode> replicateOutInsertPlans, List<RelNode> replicateOutModifyPlans) {
        if (GlobalIndexMeta.canWrite(ec, this.gsiMeta)) {
            return super.getInput(ec, duplicatedRows, classifier, outDeletePlans, outInsertPlans, outModifyPlans,
                replicateOutDeletePlans, replicateOutInsertPlans, replicateOutModifyPlans);
        } else if (GlobalIndexMeta.canDelete(ec, this.gsiMeta)) {
            // DELETE ONLY
            final ClassifyResult classified = classifier.apply(this, duplicatedRows, new ClassifyResult());

            final List<List<Object>> updateBeforeRow = new ArrayList<>(classified.updateBeforeRows);
            if (!updateBeforeRow.isEmpty()) {
                final ExecutionContext updateEc = ec.copy(new Parameters());

                final DistinctWriter deleteWriter = getDeleteWriter();

                List<RelNode> inputs = deleteWriter.getInput(updateEc, (w) -> updateBeforeRow);
                outDeletePlans
                    .addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                        Collectors.toList()));
                replicateOutDeletePlans
                    .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                        Collectors.toList()));

            }
        }

        return duplicatedRows;
    }

    @Override
    protected void addResult(List<Object> before, List<Object> after, List<Object> merged,
                             Map<Integer, ParameterContext> insertParam, boolean duplicated, boolean insertThenUpdate,
                             ExecutionContext ec, ClassifyResult result) {
        if (GlobalIndexMeta.canWrite(ec, this.gsiMeta)) {
            super.addResult(before, after, merged, insertParam, duplicated, insertThenUpdate, ec, result);
        } else if (GlobalIndexMeta.canDelete(ec, this.gsiMeta)) {
            // DELETE ONLY
            if (duplicated && !insertThenUpdate) {
                // INSERT then UPDATE means two insert rows are duplicated, just skip them both
                result.updateBeforeRows.add(before);
            }
        }
    }

    @Override
    public TableMeta getGsiMeta() {
        return gsiMeta;
    }

    @Override
    public boolean isGsiPublished(ExecutionContext ec) {
        return GlobalIndexMeta.isPublished(ec, getGsiMeta());
    }

    public DistinctWriter getDeleteWriter() {
        return deleteWriter;
    }
}
