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

package com.alibaba.polardbx.executor.mpp.planner;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.List;

public class RemoteSourceNode extends AbstractRelNode {

    private final List<Integer> sourceFragmentIds;
    private final RelCollation relCollation;
    private Integer rowCount;

    public RemoteSourceNode(RelOptCluster cluster, RelTraitSet traitSet,
                            List<Integer> sourceFragmentIds, RelDataType rowType,
                            Integer rowCount) {
        super(cluster, traitSet);
        super.rowType = rowType;
        this.relCollation = traitSet.getTrait(RelCollationTraitDef.INSTANCE);
        this.sourceFragmentIds = sourceFragmentIds;
        this.rowCount = rowCount;
    }

    /**
     * for externalize.RelJsonReader
     */
    public RemoteSourceNode(RelInput relInput) {
        super(relInput.getCluster(), relInput.getTraitSet());
        this.sourceFragmentIds = relInput.getIntegerList("fragmentIds");
        this.relCollation = relInput.getCollation();
        super.rowType = relInput.getRowType("rowType");
    }

    public List<Integer> getSourceFragmentIds() {
        return sourceFragmentIds;
    }

    public RelCollation getRelCollation() {
        return relCollation;
    }

    @Override
    public RelDataType deriveRowType() {
        return this.rowType;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // We skip the "groups" element if it is a singleton of "group".
        List<Integer> fragmentIds = new ArrayList<>(sourceFragmentIds.size());
        for (Integer id : sourceFragmentIds) {
            fragmentIds.add(id);
        }
        super.explainTerms(pw)
            .item("fragmentIds", fragmentIds)
            .item("collation", relCollation)
            .item("rowType", rowType);
        return pw;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "RemoteSource");
        pw.item("sourceFragmentIds", sourceFragmentIds);
        pw.item("type", rowType);
        return pw;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        if (rowCount == null) {
            throw new IllegalStateException("RemoteSourceNode's rowCount is NULL!");
        }
        return rowCount;
    }
}

