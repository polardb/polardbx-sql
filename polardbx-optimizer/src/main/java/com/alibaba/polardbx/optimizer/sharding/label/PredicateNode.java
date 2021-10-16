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

package com.alibaba.polardbx.optimizer.sharding.label;

import com.alibaba.polardbx.optimizer.sharding.utils.DigestCache;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.utils.LabelUtil;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.sharding.LabelBuilder;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.util.mapping.Mapping;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * @author chenmo.cm
 */
public class PredicateNode {

    private final Label baseLabel;

    /**
     * For predicates in filter or project node
     */
    private final Filter filter;
    private final List<RexNode> predicates;
    private Map<String, RexNode> digests;

    /**
     * For aggregate call
     */
    private final Aggregate aggregate;
    private final HashGroupJoin hashGroupJoin;
    private final AggregateCall aggCall;

    private final ExtractorContext context;

    public PredicateNode(Label baseLabel, Filter filter, Map<String, RexNode> digests,
                         ExtractorContext context) {
        this(baseLabel, filter, ImmutableList.copyOf(digests.values()), context);

        this.digests = digests;
    }

    public PredicateNode(Label baseLabel, Filter filter, List<RexNode> predicates, ExtractorContext context) {
        this(baseLabel, filter, predicates, (Aggregate) null, null, context);
    }

    public PredicateNode(Label baseLabel, Aggregate agg, AggregateCall aggCall, ExtractorContext context) {
        this(baseLabel, null, ImmutableList.of(), agg, aggCall, context);
    }

    public PredicateNode(Label baseLabel, HashGroupJoin agg, AggregateCall aggCall, ExtractorContext context) {
        this(baseLabel, null, ImmutableList.of(), agg, aggCall, context);
    }

    protected PredicateNode(Label baseLabel, Filter filter, List<RexNode> predicates, Aggregate agg,
                            AggregateCall aggCall, ExtractorContext context) {
        this.baseLabel = baseLabel;
        this.context = context;
        this.filter = filter;
        this.predicates = predicates;
        this.aggregate = agg;
        this.aggCall = aggCall;
        this.hashGroupJoin = null;
    }

    protected PredicateNode(Label baseLabel, Filter filter, List<RexNode> predicates, HashGroupJoin agg,
                            AggregateCall aggCall, ExtractorContext context) {
        this.baseLabel = baseLabel;
        this.context = context;
        this.filter = filter;
        this.predicates = predicates;
        this.aggregate = null;
        this.aggCall = aggCall;
        this.hashGroupJoin = agg;
    }

    public Label getBaseLabel() {
        return baseLabel;
    }

    public Filter getFilter() {
        return filter;
    }

    public List<RexNode> getPredicates() {
        return predicates;
    }

    public Map<String, RexNode> getDigests() {
        if (null == digests) {
            final DigestCache digestCache = context.getDigestCache();
            this.digests = predicates.stream()
                .collect(Collectors.toMap(digestCache::digest, rex -> rex, (rex1, rex2) -> rex1));
        }

        return digests;
    }

    public Aggregate getAggregate() {
        return aggregate;
    }

    public AggregateCall getAggCall() {
        return aggCall;
    }

    /**
     * Rebase PredicateNode to Label with same base RelNode
     *
     * @param newLabelBase Label has same base RelNode of base label of this PredicateNode
     * @return Rebased Predicates
     */
    public PredicateNode rebaseTo(Label newLabelBase) {
        return rebaseTo(newLabelBase, this.baseLabel.getColumnMapping());
    }

    /**
     * Rebase PredicateNode to new Label
     *
     * @param newLabelBase New base label
     * @param currentMapping Mapping to base RelNode of newLabelBase
     * @return Rebased predicates
     */
    public PredicateNode rebaseTo(Label newLabelBase, Mapping currentMapping) {
        final Mapping newMapping = newLabelBase.getColumnMapping();

        final Mapping multiplied = null == currentMapping ? newMapping.inverse() : LabelUtil.multiply(currentMapping,
            newMapping.inverse());

        final RexBuilder rexBuilder = this.baseLabel.getRel().getCluster().getRexBuilder();
        if (null != aggCall) {
            throw new AssertionError("Do not support rebase AggCall");
        } else {
            final List<RexNode> permuted = this.predicates.stream()
                .map(p -> LabelUtil.apply(rexBuilder, multiplied, p))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

            if (permuted.isEmpty()) {
                return null;
            } else {
                return new PredicateNode(newLabelBase, null, permuted, context);
            }
        }
    }

    public PredicateNode shift(final RexPermuteInputsShuttle permutationShuttle) {
        if (null != aggCall) {
            throw new AssertionError("Do not support shift AggCall");
        }

        final List<RexNode> shifted = getPredicates().stream()
            .filter(Objects::nonNull)
            .map(rex -> rex.accept(permutationShuttle))
            .collect(Collectors.toList());

        return new PredicateNode(getBaseLabel(), getFilter(), shifted, this.context);
    }

    /**
     * If newBaseLabel is not null rebase predicates to newBaseLabel, otherwise
     * push down predicates to relNode of newBaseLabel
     *
     * @param newBaseLabel New base label
     * @return Rebased predicates
     */
    public PredicateNode pushDown(Label newBaseLabel) {
        Mapping mapping = null;
        if (null == newBaseLabel) {
            mapping = this.baseLabel.getColumnMapping();
        } else {
            final Mapping newMapping = newBaseLabel.getColumnMapping();
            mapping = LabelUtil.multiply(this.baseLabel.getColumnMapping(), newMapping);
        }

        final RexBuilder rexBuilder = this.baseLabel.getRel().getCluster().getRexBuilder();
        if (null != aggCall) {
            throw new AssertionError("Do not support push down AggCall");
        } else {
            final Mapping finalMapping = mapping;
            final List<RexNode> permuted = this.predicates.stream()
                .map(p -> LabelUtil.apply(rexBuilder, finalMapping, p))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

            if (permuted.isEmpty()) {
                return null;
            } else {
                final LabelBuilder builder = context.labelBuilder();
                Label newBase = null;
                if (null == newBaseLabel) {
                    newBase = builder.baseOf(baseLabel);
                } else {
                    newBase = newBaseLabel.copy(newBaseLabel.getInputs());
                }
                return new PredicateNode(newBase, null, permuted, context);
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();

        if (GeneralUtil.isNotEmpty(predicates)) {
            sb.append(TStringUtil.join(getDigests().keySet(), ","));
        }

        if (null != aggCall) {
            sb.append("[").append(aggCall.toString()).append("]");
        }

        if (null != baseLabel) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(baseLabel.toString());
        }

        return sb.toString();
    }

    public void forEach(BiConsumer<String, RexNode> consumer) {
        getDigests().forEach(consumer);
    }
}
