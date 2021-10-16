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

import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorType;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.Map;

/**
 * @author chenmo.cm
 */
public class ConditionExtractor<T extends RelNode> {

    private final ExtractorContext context;
    private final T root;
    private final ExtractorType type;
    private final PredicatePullUpVisitor pullUpVisitor;
    private final PredicatePushDownVisitor pushdownVisitor;

    private Label rootLabel;

    private ConditionExtractor(T root, ExtractorType type) {
        this.root = root;
        this.type = type;
        this.context = ExtractorContext.emptyContext(type);

        switch (type) {
        case PARTITIONING_CONDITION:
        case COLUMN_EQUIVALENCE:
        case PREDICATE_MOVE_AROUND:
            pullUpVisitor = new PredicatePullUpVisitor(context);
            pushdownVisitor = new PredicatePushDownVisitor(context);
            break;
        default:
            throw new IllegalArgumentException("Unsupported extractor type " + type.toString());
        }
    }

    public static ConditionExtractor<RelNode> partitioningConditionFrom(RelNode root) {
        return new ConditionExtractor<>(root, ExtractorType.PARTITIONING_CONDITION);
    }

    public static ConditionExtractor<RelNode> columnEquivalenceFrom(RelNode root) {
        return new ConditionExtractor<>(root, ExtractorType.COLUMN_EQUIVALENCE);
    }

    public static ConditionExtractor<RelNode> predicateFrom(RelNode root) {
        return new ConditionExtractor<>(root, ExtractorType.PREDICATE_MOVE_AROUND);
    }

    public ExtractionResult extract() {
        this.rootLabel = labelInit(root, type);

        if (null != rootLabel) {
            pushDown(pullUp(rootLabel));
        }

        return new ExtractionResult(this);
    }

    private Label labelInit(T root, ExtractorType type) {
        final RelToLabelConverter relToLabelConverter = new RelToLabelConverter(type, context);

        RelNode rel = root;
        if (root instanceof BaseTableOperation) {
            if (null == ((BaseTableOperation) root).getParent()) {
                return null;
            }
            rel = ((BaseTableOperation) root).getParent();
        }

        rel.accept(relToLabelConverter);
        return relToLabelConverter.getRoot();
    }

    private Label pullUp(Label rootLabel) {
        return rootLabel.accept(this.pullUpVisitor);
    }

    private Label pushDown(Label rootLabel) {
        return rootLabel.accept(this.pushdownVisitor);
    }

    public Map<RelNode, Label> getRelLabelMap() {
        return pullUpVisitor.getRelLabelMap();
    }

    public Map<RelOptTable, Map<RelNode, Label>> getTableLabelMap() {
        return pushdownVisitor.getTableLabelMap();
    }

    public Label getRootLabel() {
        return rootLabel;
    }

    public ExtractorType getType() {
        return type;
    }

    public ExtractorContext getContext() {
        return context;
    }
}
