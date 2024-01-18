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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableModifyBuilder;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * UPDATE/DELETE on one single table with primary key specified
 *
 * @author chenmo.cm
 */
public class SingleModifyWriter extends AbstractSingleWriter implements DistinctWriter {

    /**
     * Operator for UPDATE/DELETE
     */
    private final LogicalModify modify;
    /**
     * Mapping for primary key in input row
     */
    private final Mapping pkMapping;
    /**
     * Mapping for source value of SET in input row
     * Null if type is DELETE
     */
    private Mapping updateSetMapping;
    /**
     * Mapping for columns used to group input rows, normally identical to pkMapping
     * if pkMapping and skMapping are identical, then groupingMapping is also identical to them
     * otherwise groupingMapping is the combination of pkMapping and skMapping
     */
    private final Mapping groupingMapping;

    private final boolean withoutPk;

    public SingleModifyWriter(RelOptTable targetTable, LogicalModify modify, Mapping pkMapping,
                              Mapping updateSetMapping, Mapping groupingMapping, boolean withoutPk) {
        super(targetTable, modify.getOperation());
        this.modify = modify;
        this.pkMapping = pkMapping;
        this.updateSetMapping = updateSetMapping;
        this.groupingMapping = groupingMapping;
        this.withoutPk = withoutPk;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext ec, Function<DistinctWriter, List<List<Object>>> rowGenerator) {
        final RelOptTable targetTable = getTargetTable();
        final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);
        final String schemaName = qn.left;
        final String logicalTableName = qn.right;

        // Deduplicate
        final List<List<Object>> distinctRows = rowGenerator.apply(this);
        if (distinctRows.isEmpty()) {
            return new ArrayList<>();
        }

        List<Integer> pkIndexList = new ArrayList<>();
        pkMapping.iterator().forEachRemaining(e -> pkIndexList.add(e.target));

        // targetDb: { targetTb: [{ rowIndex, [pk1, pk2] }] }
        final Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResult = BuildPlanUtils
            .buildResultForSingleTable(schemaName, logicalTableName, distinctRows, pkIndexList, ec);

        final PhyTableModifyBuilder builder = new PhyTableModifyBuilder();
        switch (getOperation()) {
        case UPDATE:
            return builder.buildUpdateWithPk(modify, distinctRows, updateSetMapping, qn, shardResult, ec);
        case DELETE:
            return builder.buildDelete(modify, qn, shardResult, ec, withoutPk);
        default:
            throw new AssertionError("Cannot handle operation " + getOperation().name());
        }
    }

    public Mapping getUpdateSetMapping() {
        return updateSetMapping;
    }

    public void setUpdateSetMapping(Mapping updateSetMapping) {
        this.updateSetMapping = updateSetMapping;
    }

    @Override
    public Mapping getGroupingMapping() {
        return groupingMapping;
    }

    public LogicalModify getModify() {
        return modify;
    }

    @Override
    public List<Writer> getInputs() {
        List writerList = Collections.emptyList();
        return writerList;
    }
}
