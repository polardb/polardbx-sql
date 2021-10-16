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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.SingleWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.List;
import java.util.function.Function;

/**
 * @author chenmo.cm
 */
public class DistinctWriterWrapper extends AbstractSingleWriter implements DistinctWriter {

    private final Mapping deduplicateMapping;
    private final SingleWriter writer;

    public DistinctWriterWrapper(SingleWriter writer, Mapping deduplicateMapping) {
        super(writer.getTargetTable(), writer.getOperation());
        Preconditions.checkNotNull(writer);

        this.writer = writer;
        this.deduplicateMapping = deduplicateMapping;
    }

    @Override
    public Mapping getGroupingMapping() {
        return deduplicateMapping;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext ec, Function<DistinctWriter, List<List<Object>>> rowGenerator) {
        throw new UnsupportedOperationException("DistinctWriterWrapper do not support getInput");
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(writer)) {
            return (C) writer;
        }
        return super.unwrap(aClass);
    }

    public static DistinctWriterWrapper wrap(InsertWriter insertWriter) {
        final int fieldCount = insertWriter.getInsert().getInsertRowType().getFieldCount();

        final Mapping identityMapping = Mappings.target(Mappings.createIdentity(fieldCount), fieldCount, fieldCount);
        return new DistinctWriterWrapper(insertWriter, identityMapping);
    }

    @Override
    public List<Writer> getInputs() {
        final List<Writer> writerList = Lists.newArrayList();
        writerList.add(writer);
        return writerList;
    }
}
