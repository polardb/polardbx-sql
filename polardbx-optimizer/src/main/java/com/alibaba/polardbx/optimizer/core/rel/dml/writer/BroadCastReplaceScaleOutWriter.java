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

import com.google.common.collect.Lists;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.BroadcastWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.DuplicateCheckResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class BroadCastReplaceScaleOutWriter extends AbstractSingleWriter implements BroadcastWriter {
    private final LogicalInsert parent;
    final BroadcastInsertWriter insertWriter;
    final BroadcastModifyWriter deleteWriter;

    public BroadCastReplaceScaleOutWriter(LogicalInsert parent,
                                          RelOptTable targetTable,
                                          BroadcastInsertWriter insertWriter,
                                          BroadcastModifyWriter deleteWriter) {
        super(targetTable, TableModify.Operation.UPDATE);
        this.parent = parent;
        this.insertWriter = insertWriter;
        this.deleteWriter = deleteWriter;
    }

    public SourceRows getInput(ExecutionContext ec, ExecutionContext insertEc,
                               Function<DistinctWriter, SourceRows> rowGenerator,
                               List<RelNode> outDeletePlans,
                               List<RelNode> outInsertPlans) {
        final SourceRows distinctRows = rowGenerator.apply(getDeleteWriter());
        final List<DuplicateCheckResult> values = distinctRows.valueRows;
        final List<List<Object>> relocateRows = new ArrayList<>();
        values.forEach(row -> {
            if (row.duplicated) {
                relocateRows.add(row.before);
            }
        });

        if (!relocateRows.isEmpty()) {
            outDeletePlans.addAll(getDeleteWriter().getInput(ec, (w) -> relocateRows));
        }
        outInsertPlans.addAll(getInsertWriter().getInput(insertEc));

        return distinctRows;
    }

    public BroadcastInsertWriter getInsertWriter() {
        return insertWriter;
    }

    public BroadcastModifyWriter getDeleteWriter() {
        return deleteWriter;
    }

    @Override
    public List<Writer> getInputs() {
        final List<Writer> writerList = Lists.newArrayList();
        writerList.add(insertWriter);
        writerList.add(deleteWriter);
        return writerList;
    }

}
