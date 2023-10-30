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

package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.sql.OutFileParams;

import java.util.List;

public class LogicalOutFile extends SingleRel {
    private OutFileParams outFileParams;

    /**
     * Creates a LogicalOutFile.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traitSet Traits of this relational expression
     * @param input Input relational expression
     * @param outFileParams parameters used to control the format of the out file
     */
    public LogicalOutFile(RelOptCluster cluster,
                          RelTraitSet traitSet,
                          RelNode input,
                          OutFileParams outFileParams) {
        super(cluster, traitSet, input);
        this.outFileParams = outFileParams;
        this.rowType = input.getRowType();
    }

    public void setOutFileParams(OutFileParams outFileParams) {
        this.outFileParams = outFileParams;
    }

    public OutFileParams getOutFileParams() {
        return outFileParams;
    }

    public LogicalOutFile copy(RelTraitSet traitSet, RelNode input) {
        return new LogicalOutFile(getCluster(), traitSet, input, outFileParams);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("LogicalOutFile", outFileParams.getFileName());
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "LogicalOutFile");

        pw.item("outfile name", outFileParams.getFileName());

        return pw;
    }

    @Override
    public LogicalOutFile copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalOutFile(getCluster(), traitSet, sole(inputs), outFileParams);
    }
}
