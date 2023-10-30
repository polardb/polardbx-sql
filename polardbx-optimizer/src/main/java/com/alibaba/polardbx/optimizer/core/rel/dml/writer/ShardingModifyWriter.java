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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableModifyBuilder;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;

/**
 * UPDATE/DELETE on one sharding table with primary key or unique key specified
 * <p>
 * Presupposed input row constitution:
 * <pre>
 *  UPDATE t SET c1 = u1, ..., cm = um
 *
 *  Input row looks like below
 *
 *           new values in SET statement
 *                      |
 *    [c1, ..., cn][u1, ..., um]
 *          |
 *  selected before values from source of update
 * </pre>
 *
 * <pre>
 *  DELETE FROM t WHERE
 *
 *  Input row looks like below
 *
 *    [c1, ..., cn]
 *          |
 *  selected before values from source of delete
 * </pre>
 *
 * @author chenmo.cm
 */
public class ShardingModifyWriter extends AbstractSingleWriter implements DistinctWriter {

    /**
     * Operator for UPDATE/DELETE
     */
    protected final LogicalModify modify;
    /**
     * Column meta for sharding, identical to column metas in TableMeta
     */
    protected final List<ColumnMeta> skMetas;
    /**
     * Mapping for primary key in input row
     */
    protected final Mapping pkMapping;
    /**
     * Mapping for sharding key in input row
     */
    protected final Mapping skMapping;
    /**
     * Mapping for source value of SET in input row
     * Null if type is DELETE
     */
    protected Mapping updateSetMapping;
    /**
     * Mapping for columns used to group input rows, normally identical to pkMapping
     * if pkMapping and skMapping are identical, then groupingMapping is also identical to them
     * otherwise groupingMapping is the combination of pkMapping and skMapping
     */
    protected final Mapping groupingMapping;

    protected final boolean withoutPk;

    public ShardingModifyWriter(RelOptTable targetTable, LogicalModify modify, List<ColumnMeta> skMetas,
                                Mapping pkMapping, Mapping skMapping, Mapping updateSetMapping,
                                Mapping groupingMapping, boolean withoutPk) {
        super(targetTable, modify.getOperation());
        this.modify = modify;
        this.skMetas = skMetas;
        this.pkMapping = pkMapping;
        this.skMapping = skMapping;
        this.updateSetMapping = updateSetMapping;
        this.groupingMapping = groupingMapping;
        this.withoutPk = withoutPk;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext ec, Function<DistinctWriter, List<List<Object>>> rowGenerator) {
        final RelOptTable targetTable = getTargetTable();
        final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);

        // Deduplicate
        final List<List<Object>> distinctRows = rowGenerator.apply(this);

        if (distinctRows.isEmpty()) {
            return new ArrayList<>();
        }

        // targetDb: { targetTb: [{ rowIndex, [pk1, pk2] }] }
        final Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResult = BuildPlanUtils
            .buildResultForShardingTable(qn.left, qn.right, distinctRows, skMetas, skMapping, pkMapping, ec, false);

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

    public LogicalModify getModify() {
        return modify;
    }

    public List<ColumnMeta> getSkMetas() {
        return skMetas;
    }

    public Mapping getPkMapping() {
        return pkMapping;
    }

    public Mapping getSkMapping() {
        return skMapping;
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

    @Override
    public List<Writer> getInputs() {
        return Collections.emptyList();
    }
}
