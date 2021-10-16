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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.utils.LabelUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.mapping.Mapping;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * @author chenmo.cm
 */
public abstract class AbstractLabelOptNode implements Label {

    private final LabelType type;
    private final List<Label> inputs;

    /**
     * Bottom node of the operator subtree which belongs to this label
     */
    protected final RelNode rel;

    /**
     * For Semi/Anti Join
     */
    protected final FullRowType fullRowType;

    /**
     * Column Mapping from current projected rowType to origin rowType of
     * TableScan/Join/ProjectFilter
     */
    protected Mapping columnMapping;
    /**
     * Current projected rowType
     */
    protected RelDataType currentBaseRowType;

    /**
     * RowType cache
     */
    protected RelDataType rowType = null;

    /**
     * Pull all filters up, above the top project of current input, then cache them
     * here
     */
    protected PredicateNode pullUp;

    /**
     * Rebase pushdown conditions to {@link #currentBaseRowType}, and cache them here
     */
    protected PredicateNode pushdown;
    protected PredicateNode correlatePushdown;

    /**
     * For predicates like '$2 and $3 > 1', in which '$2' represents an boolean
     * expression
     */
    protected PredicateNode[] columnConditionMap;

    /**
     * Predicates directly on top of to this label
     */
    protected List<PredicateNode> predicates;

    protected AbstractLabelOptNode(LabelType type, @Nonnull RelNode rel, List<Label> inputs) {
        this.type = type;
        this.rel = rel;
        this.inputs = inputs;
        this.currentBaseRowType = rel.getRowType();
        this.columnMapping = LabelUtil.identityColumnMapping(this.currentBaseRowType.getFieldCount());
        this.fullRowType = FullRowType.create(type, rel.getRowType(), inputs, rel.getCluster().getTypeFactory());
        this.columnConditionMap = new PredicateNode[columnMapping.getSourceCount()];
        this.predicates = new ArrayList<>();
    }

    protected AbstractLabelOptNode(LabelType type, List<Label> inputs, RelNode rel, FullRowType fullRowType,
                                   Mapping columnMapping, RelDataType currentBaseRowType, PredicateNode pullUp,
                                   PredicateNode pushdown, PredicateNode[] columnConditionMap,
                                   List<PredicateNode> predicates) {
        this.type = type;
        this.inputs = inputs;
        this.rel = rel;
        this.fullRowType = fullRowType;
        this.columnMapping = columnMapping;
        this.currentBaseRowType = currentBaseRowType;
        this.pullUp = pullUp;
        this.pushdown = pushdown;
        this.columnConditionMap = columnConditionMap;
        this.predicates = predicates;
    }

    @Override
    public RelDataType getRowType() {
        if (null == this.rowType) {
            this.rowType = deriveRowType();
        }
        return this.rowType;
    }

    @Override
    public RelDataType deriveRowType() {
        return deriveRowType(getRel().getCluster().getTypeFactory(), getRel().getRowType().getFieldList());
    }

    protected RelDataType deriveRowType(RelDataTypeFactory factory, List<RelDataTypeField> baseFields) {
        final List<RelDataTypeField> fields = LabelUtil
            .permuteColumns(columnMapping, currentBaseRowType.getFieldList(), baseFields);
        return factory.createStructType(fields);
    }

    @Override
    public boolean equalsOpt(LabelOptNode that) {
        if (!this.equals(that)) {
            return false;
        }

        if (null == this.rowType || null == that.getRowType()) {
            return false;
        }

        return this.rowType == that.getRowType();
    }

    @Override
    public Label clone() {
        try {
            super.clone();
        } catch (CloneNotSupportedException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, e);
        }
        return copy(getInputs());
    }

    @Override
    public List<Label> getInputs() {
        return this.inputs;
    }

    @Override
    public <T extends Label> T getInput(int i) {
        if (null == this.inputs || this.inputs.size() <= i) {
            return null;
        }

        return (T) this.inputs.get(i);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Label)) {
            return false;
        }
        Label label = (Label) o;
        return getRel().equals(label.getRel());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRel());
    }

    @Override
    public String toString() {
        return TStringUtil.substringAfter(super.toString(), "@") + "@" + type.name();
    }

    @Override
    public boolean is(EnumSet<LabelType> typeSet) {
        return typeSet.contains(this.getType());
    }

    @Override
    public <R extends RelNode> R getRel() {
        return (R) this.rel;
    }

    @Override
    public LabelType getType() {
        return type;
    }

    @Override
    public FullRowType getFullRowType() {
        return fullRowType;
    }

    @Override
    public Mapping getColumnMapping() {
        return columnMapping;
    }

    @Override
    public PredicateNode[] getColumnConditionMap() {
        return columnConditionMap;
    }

    @Override
    public boolean withoutExpressionColumn() {
        return !withExpressionColumn();
    }

    @Override
    public boolean withExpressionColumn() {
        return Arrays.stream(this.getColumnConditionMap()).anyMatch(Objects::nonNull);
    }

    @Override
    public PredicateNode getPullUp() {
        return pullUp;
    }

    @Override
    public void setPullUp(PredicateNode pullUp) {
        this.pullUp = pullUp;
    }

    @Override
    public PredicateNode getPushdown() {
        return pushdown;
    }

    @Override
    public void setPushdown(PredicateNode pushdown) {
        this.pushdown = pushdown;
    }

    @Override
    public List<PredicateNode> getPredicates() {
        return predicates;
    }

    public abstract Label aggregate(HashGroupJoin agg, ExtractorContext context);

    @Override
    public PredicateNode getCorrelatePushdown() {
        return correlatePushdown;
    }

    @Override
    public void setCorrelatePushdown(PredicateNode correlatePushdown) {
        this.correlatePushdown = correlatePushdown;
    }
}
