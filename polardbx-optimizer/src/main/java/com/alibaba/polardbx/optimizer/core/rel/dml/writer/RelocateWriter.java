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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.dml.CaseWhenWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.ClassifyResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RowClassifier;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;

import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Writer for modify sharding column on logical table, do not take gsi into consideration
 *
 * @author chenmo.cm
 */
public class RelocateWriter extends AbstractSingleWriter implements CaseWhenWriter {

    private final DistinctWriter deleteWriter;
    private final DistinctWriter insertWriter;
    private final DistinctWriter modifyWriter;

    // the sk is the identifier key, the pk could be the identifier key in ScaleOut/GSI writable phase
    protected final Mapping identifierKeyTargetMapping;
    protected final Mapping identifierKeySourceMapping;
    protected final List<ColumnMeta> identifierKeyMetas;
    protected final boolean modifySkOnly;
    protected final boolean usePartFieldChecker;

    public RelocateWriter(RelOptTable targetTable, DistinctWriter deleteWriter, DistinctWriter insertWriter,
                          DistinctWriter modifyWriter, Mapping identifierKeyTargetMapping,
                          Mapping identifierKeySourceMapping,
                          List<ColumnMeta> identifierKeyMetas, boolean modifySkOnly, boolean usePartFieldChecker) {
        super(targetTable, Operation.UPDATE);

        this.deleteWriter = deleteWriter;
        this.insertWriter = insertWriter;
        this.modifyWriter = modifyWriter;
        this.identifierKeyTargetMapping = identifierKeyTargetMapping;
        this.identifierKeySourceMapping = identifierKeySourceMapping;
        this.identifierKeyMetas = identifierKeyMetas;
        this.modifySkOnly = modifySkOnly;
        this.usePartFieldChecker = usePartFieldChecker;
    }

    public DistinctWriter getDeleteWriter() {
        return deleteWriter;
    }

    public DistinctWriter getInsertWriter() {
        return insertWriter;
    }

    public DistinctWriter getModifyWriter() {
        return modifyWriter;
    }

    public Mapping getIdentifierKeyTargetMapping() {
        return identifierKeyTargetMapping;
    }

    public Mapping getIdentifierKeySourceMapping() {
        return identifierKeySourceMapping;
    }

    public List<ColumnMeta> getIdentifierKeyMetas() {
        return identifierKeyMetas;
    }

    public boolean getModifySkOnly() {
        return modifySkOnly;
    }

    public boolean isUsePartFieldChecker() {
        return usePartFieldChecker;
    }

    public SourceRows getInput(ExecutionContext ec, ExecutionContext insertEc,
                               Function<DistinctWriter, SourceRows> rowGenerator,
                               RowClassifier classifier, List<RelNode> outDeletePlans,
                               List<RelNode> outInsertPlans, List<RelNode> outModifyPlans,
                               List<RelNode> replicateOutDeletePlans, List<RelNode> replicateOutInsertPlans,
                               List<RelNode> replicateOutModifyPlans) {
        final SourceRows sourceRows = rowGenerator.apply(getDeleteWriter());

        final ClassifyResult classifyResult = classifier.apply(this, sourceRows, new ClassifyResult());
        final List<List<Object>> modifyRows = classifyResult.modifyRows;
        final List<List<Object>> relocateRows = classifyResult.relocateRows;

        List<RelNode> inputs = getModifyWriter().getInput(ec, (w) -> modifyRows);
        outModifyPlans.addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
            Collectors.toList()));
        replicateOutModifyPlans
            .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));

        inputs = getDeleteWriter().getInput(ec, (w) -> relocateRows);
        outDeletePlans.addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
            Collectors.toList()));
        replicateOutDeletePlans
            .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));

        inputs = getInsertWriter().getInput(insertEc, (w) -> relocateRows);
        outInsertPlans.addAll(inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
            Collectors.toList()));
        replicateOutInsertPlans
            .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));

        return sourceRows;
    }

    /**
     * If the old and new value of all sharding column is identical, use UPDATE/REPLACE instead of DELETE + INSERT.
     * If nothing has changed, just skip this row.
     *
     * @param identicalSk Whether old and new value of all sharding column is identical
     * @param sourceRows Input rows
     */
    @Override
    public ClassifyResult classify(BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> identicalSk,
                                   SourceRows sourceRows, ExecutionContext ec, ClassifyResult result) {
        for (List<Object> row : sourceRows.selectedRows) {
            if (identicalSk.test(this, Pair.of(row, ImmutableMap.of()))) {
                if (!modifySkOnly) {
                    result.modifyRows.add(row);
                }
            } else {
                result.relocateRows.add(row);
            }
        }

        return result;
    }

    @Override
    public List<Writer> getInputs() {
        final List<Writer> writerList = Lists.newArrayList();
        writerList.add(insertWriter);
        writerList.add(deleteWriter);
        writerList.add(modifyWriter);
        return writerList;
    }

    public static boolean checkSkUsePartField(List<Object> srcObject, List<Object> tarObject,
                                              List<DataType> srcDataType, List<DataType> tarDataType,
                                              List<DataType> skDataType, ExecutionContext executionContext) {
        final int fieldCnt = srcObject.size();
        for (int i = 0; i < fieldCnt; i++) {
            try {
                PartitionField srcPartField =
                    PartitionPrunerUtils.buildPartField(srcObject.get(i), srcDataType.get(i), skDataType.get(i), null,
                        executionContext, PartFieldAccessType.DML_PRUNING);
                PartitionField tarPartField =
                    PartitionPrunerUtils.buildPartField(tarObject.get(i), tarDataType.get(i), skDataType.get(i), null,
                        executionContext, PartFieldAccessType.DML_PRUNING);

                if (tarPartField.isNull() || srcPartField.isNull()) {
                    if (tarPartField.isNull() && srcPartField.isNull()) {
                        continue;
                    }
                    return false;
                }

                // Compare bytes directly, since 2 fields' type must be same
                if (memCmp(srcPartField.rawBytes(), tarPartField.rawBytes()) != 0) {
                    return false;
                }
            } catch (Throwable ex) {
                // Can not convert, just use delete + insert
                return false;
            }
        }

        return true;
    }

    private static int memCmp(byte[] left, byte[] right) {
        int minLen = Math.min(left.length, right.length);
        int index = 0;
        while (index < minLen - 1 && left[index] == right[index]) {
            index++;
        }
        return left[index] - right[index];
    }
}
