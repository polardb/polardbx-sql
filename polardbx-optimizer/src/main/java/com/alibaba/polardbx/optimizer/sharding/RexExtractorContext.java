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

package com.alibaba.polardbx.optimizer.sharding;

import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorType;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.utils.Assert;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * @author chenmo.cm
 */
public class RexExtractorContext {

    private RexExtractor rexExtractor;
    private SubqueryRexExtractor subqueryExtractor;
    private RelToLabelConverter relToLabelConverter;

    private final RexBuilder builder;
    private final Deque<Boolean> operandOfOr = new ArrayDeque<>(ImmutableList.of(false));

    public static RexExtractorContext create(ExtractorType type, RelToLabelConverter relToLabelConverter,
                                             RelNode relNode) {
        final RexBuilder builder = relNode.getCluster().getRexBuilder();
        final RexExtractorContext context = new RexExtractorContext(builder, relToLabelConverter);
        switch (type) {
        case PARTITIONING_CONDITION:
            context.setRexExtractor(new ShardingRexExtractor(context));
            context.setSubqueryExtractor(new SubqueryRexExtractor(relNode, context));
            break;
        case COLUMN_EQUIVALENCE:
            context.setRexExtractor(new ColumnEqualityRexExtractor(context));
            context.setSubqueryExtractor(new SubqueryRexExtractor(relNode, context));
            break;
        case PREDICATE_MOVE_AROUND:
            context.setRexExtractor(new RexExtractor(context));
            context.setSubqueryExtractor(new SubqueryRexExtractor(relNode, context));
            break;
        default:
            Assert.fail("Unknown ExtractorType: " + type);
            return null;
        }

        return context;
    }

    public RexExtractorContext(RexBuilder builder) {
        this.builder = builder;
        this.relToLabelConverter = null;
    }

    protected RexExtractorContext(RexBuilder builder, RelToLabelConverter relToLabelConverter) {
        this.builder = builder;
        this.relToLabelConverter = relToLabelConverter;
    }

    public RexExtractorContext(RexExtractor rexExtractor, SubqueryRexExtractor subqueryExtractor, RexBuilder builder) {
        this.rexExtractor = rexExtractor;
        this.subqueryExtractor = subqueryExtractor;
        this.builder = builder;
    }

    protected void setRexExtractor(RexExtractor rexExtractor) {
        this.rexExtractor = rexExtractor;
    }

    protected void setSubqueryExtractor(SubqueryRexExtractor subqueryExtractor) {
        this.subqueryExtractor = subqueryExtractor;
    }

    public RexExtractor getRexExtractor() {
        return rexExtractor;
    }

    public SubqueryRexExtractor getSubqueryExtractor() {
        return subqueryExtractor;
    }

    public RelToLabelConverter getRelToLabelConverter() {
        return relToLabelConverter;
    }

    public RexBuilder getBuilder() {
        return builder;
    }

    public Deque<Boolean> getOperandOfOr() {
        return operandOfOr;
    }

    public boolean isOperandOfOr() {
        return operandOfOr.peek();
    }
}
