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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.BroadcastWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.GsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.util.mapping.Mapping;

import java.util.List;
import java.util.function.Function;

/**
 * @author chenmo.cm
 */
public class BroadcastModifyGsiWriter extends AbstractSingleWriter
    implements DistinctWriter, GsiWriter, BroadcastWriter {

    protected final BroadcastModifyWriter deleteWriter;
    protected final BroadcastModifyWriter updateWriter;

    protected final TableMeta gsiMeta;

    public BroadcastModifyGsiWriter(RelOptTable targetTable, Operation op, BroadcastModifyWriter updateWriter,
                                    BroadcastModifyWriter deleteWriter, TableMeta gsiMeta) {
        super(targetTable, op);
        this.deleteWriter = deleteWriter;
        this.updateWriter = updateWriter;
        this.gsiMeta = gsiMeta;
    }

    public BroadcastModifyWriter getDeleteWriter() {
        return deleteWriter;
    }

    public BroadcastModifyWriter getUpdateWriter() {
        return updateWriter;
    }

    @Override
    public TableMeta getGsiMeta() {
        return gsiMeta;
    }

    @Override
    public boolean isGsiPublished(ExecutionContext ec) {
        return GlobalIndexMeta.isPublished(ec, getGsiMeta());
    }

    @Override
    public Mapping getGroupingMapping() {
        return deleteWriter.getGroupingMapping();
    }

    @Override
    public List<RelNode> getInput(ExecutionContext ec, Function<DistinctWriter, List<List<Object>>> rowGenerator) {

        switch (getOperation()) {
        case UPDATE:
            if (GlobalIndexMeta.canWrite(ec, gsiMeta) || GlobalIndexMeta.canDelete(ec, gsiMeta)) {
                // WRITE_ONLY or PUBLIC
                return updateWriter.getInput(ec, rowGenerator);
            }
            // Check DELETE_ONLY
        case DELETE:
            if (GlobalIndexMeta.canDelete(ec, gsiMeta)) {
                // DELETE_ONLY
                return deleteWriter.getInput(ec, rowGenerator);
            } else {
                // Skip
                return ImmutableList.of();
            }
        default:
            throw new AssertionError("Cannot handle operation " + getOperation().name());
        }
    }

    @Override
    public List<Writer> getInputs() {
        final List<Writer> writerList = Lists.newArrayList();
        writerList.add(deleteWriter);
        writerList.add(updateWriter);
        return writerList;
    }

}
