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

package com.alibaba.polardbx.optimizer.core.planner.rule.mpp;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class RuleUtils {
    public static boolean satisfyDistribution(RelDistribution requestTrait, RelNode childNode) {
        return childNode.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE).satisfies(requestTrait);
    }

    public static boolean satisfyCollation(RelCollation requestTrait, RelNode childNode) {
        return childNode.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE).satisfies(requestTrait);
    }

    public static boolean sameDateType(RelNode input, RelDataType keyDataType, List<Integer> keys) {
        List<RelDataTypeField> keyDataFieldList = keyDataType.getFieldList();

        for (int i = 0; i < keyDataFieldList.size(); i++) {
            RelDataType t1 = keyDataFieldList.get(i).getType();
            RelDataTypeField t2Field = input.getRowType().getFieldList().get(keys.get(i));
            RelDataType t2 = t2Field.getType();
            if (!sameType(t1, t2)) {
                return false;
            }
        }
        return true;
    }

    public static boolean sameType(RelDataType t1, RelDataType t2) {
        // both are character type(char and varchar), should consider collation here
        if (SqlTypeUtil.isCharacter(t1) && SqlTypeUtil.isCharacter(t2)) {
            // charset or collation is null, should not reach here
            if (t1.getCharset() == null || t1.getCollation() == null) {
                return false;
            }
            boolean collationEquals =
                t1.getCharset().equals(t2.getCharset()) && t1.getCollation().equals(t2.getCollation());
            if (!collationEquals) {
                return false;
            }
        } else if (!t1.getSqlTypeName().equals(t2.getSqlTypeName())) {
            // only compare sql type name
            return false;
        }
        return true;
    }

    public static RelNode ensureKeyDataTypeDistribution(RelNode input, RelDataType keyDataType, List<Integer> keys) {
        List<Integer> outputKeys = new ArrayList<>();
        List<RelDataTypeField> keyDataFieldList = keyDataType.getFieldList();
        boolean needProject = false;

        List<RexNode> identityProjectChildExps = new ArrayList<>();
        List<RexNode> shuffleProjectChildExps = new ArrayList<>();
        List<RelDataTypeField> shuffleProjectRelDataTypeFieldList = new ArrayList<>(input.getRowType().getFieldList());
        // identity project
        for (int i = 0; i < input.getRowType().getFieldCount(); i++) {
            RexNode rexNode = new RexInputRef(i, input.getRowType().getFieldList().get(i).getType());
            shuffleProjectChildExps.add(rexNode);
            identityProjectChildExps.add(rexNode);
        }

        RexBuilder rexBuilder = input.getCluster().getRexBuilder();

        for (int i = 0; i < keyDataFieldList.size(); i++) {
            RelDataType t1 = keyDataFieldList.get(i).getType();
            RelDataTypeField t2Field = input.getRowType().getFieldList().get(keys.get(i));
            RelDataType t2 = t2Field.getType();
            // only compare sql type name
            if (!t1.getSqlTypeName().equals(t2.getSqlTypeName())) {
                needProject = true;
                outputKeys.add(shuffleProjectChildExps.size());
                RexNode expr = shuffleProjectChildExps.get(keys.get(i));
                RexNode castRexNode = rexBuilder.makeCastForConvertlet(t1, expr);
                shuffleProjectChildExps.add(castRexNode);
                shuffleProjectRelDataTypeFieldList.add(new RelDataTypeFieldImpl(t2Field.getName() + "_shuffle__",
                    shuffleProjectRelDataTypeFieldList.size(), castRexNode.getType()));
            } else {
                outputKeys.add(keys.get(i));
            }
        }

        RelDistribution distribution = RelDistributions.hash(outputKeys);

        if (needProject) {
            RelDataType shuffleProjectRowType = new RelRecordType(shuffleProjectRelDataTypeFieldList);
            RelNode shuffleProject = LogicalProject.create(input, shuffleProjectChildExps, shuffleProjectRowType);
            shuffleProject = shuffleProject.copy(input.getTraitSet(), shuffleProject.getInputs());
            shuffleProject = RelOptRule.convert(shuffleProject, shuffleProject.getTraitSet().replace(distribution));
            LogicalProject identityProject = LogicalProject.create(shuffleProject, identityProjectChildExps,
                input.getRowType().getFieldNames());
            return identityProject.copy(input.getTraitSet().replace(RelDistributions.ANY), identityProject.getInput(),
                identityProject.getProjects(), identityProject.getRowType());
        } else {
            return RelOptRule.convert(input, input.getTraitSet().replace(distribution));
        }
    }

    public static RexNode getPartialFetch(Sort sort) {
        RexBuilder builder = sort.getCluster().getRexBuilder();
        RexNode fetch = sort.fetch;
        if (sort.offset != null && sort.fetch != null) {
            Map<Integer, ParameterContext> parameterContextMap = PlannerContext.getPlannerContext(
                sort).getParams().getCurrentParameter();

            if (sort.fetch instanceof RexDynamicParam || sort.offset instanceof RexDynamicParam) {
                /**
                 * fetch or offset be parameterized.
                 */
                fetch = builder.makeCall(SqlStdOperatorTable.PLUS, fetch, sort.offset);

            } else {
                long fetchVal = CBOUtil.getRexParam(sort.fetch, parameterContextMap);
                long offsetVal = CBOUtil.getRexParam(sort.offset, parameterContextMap);
                /**
                 * fetch or offset be parameterized.
                 */
                fetch = builder.makeBigIntLiteral(offsetVal + fetchVal);
            }
        }
        return fetch;
    }
}
