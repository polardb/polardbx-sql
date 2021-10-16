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

import com.alibaba.polardbx.optimizer.sharding.utils.LabelUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableBitSet.Builder;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class FullRowType {

    /**
     * Semi/Anti join do not output columns of right input. For predicate inference,
     * we have to maintain a full rowType with all column of left and right input
     * included
     */
    public final RelDataType fullRowType;
    /**
     * Column Mapping from origin rowType of TableScan/Join/ProjectFilter to
     * {@link #fullRowType}
     */
    private final Mapping fullColumnMapping;
    /**
     * Mark all real-column (which exists in a real table) among keys of
     * {@link #fullRowType}
     */
    private final ImmutableBitSet columnInputSet;

    /**
     * Column equality conditions on fullRowType. For {@link FullRowType} created
     * with {@link FullRowType#derive(Label)}, new FullRowType object share SAME
     * columnInputSet object with its input label
     */
    private final Map<String, RexNode> columnEqualities;

    private FullRowType(RelDataType fullRowType, Mapping fullColumnMapping, ImmutableBitSet columnInputSet,
                        Map<String, RexNode> columnEqualities) {
        this.fullRowType = fullRowType;
        this.fullColumnMapping = fullColumnMapping;
        this.columnInputSet = columnInputSet;
        this.columnEqualities = columnEqualities;
    }

    public static FullRowType createSnapshot(Label input) {
        return derive(input);
    }

    public static FullRowType create(LabelType type, RelDataType bottomRowType, List<Label> inputs,
                                     RelDataTypeFactory typeFactory) {
        if (type.isLeafLabel()) {
            return identity(bottomRowType);
        } else if (type.isSnapshotLabel()) {
            return derive((inputs.get(0)));
        } else if (type.isColumnJointLabel()) {
            if (type == LabelType.CORRELATE) {
                return apply(typeFactory, bottomRowType, inputs);
            }
            return concat(typeFactory, inputs);
        } else {
            // ie. UnionLabel
            return identity(bottomRowType);
        }
    }

    /**
     * Build fullColumnMapping by concatenate fullColumnMapping from inputs
     */
    private static FullRowType apply(RelDataTypeFactory typeFactory, RelDataType bottomRowType, List<Label> inputs) {
        final ImmutableMap.Builder<Integer, Integer> newFullColumnIndexMap = ImmutableMap.builder();
        final ImmutableBitSet.Builder newColumnInputSet = ImmutableBitSet.builder();

        // build full row type
        final RelDataType newFullRowType = LabelUtil.concatApplyRowTypes(inputs, typeFactory,
            bottomRowType.getFieldList().get(bottomRowType.getFieldCount() - 1));

        // build fullColumnIndexMap & columnInputSet
        int topShift = 0;
        int bottomShift = 0;
        int index = 0;
        for (final Label rightInput : inputs) {
//            if (index++ == 1) {
//                topShift += 1;
//                bottomShift += 1;
//            }
            final Mapping currentColumnIndex = rightInput.getColumnMapping();
            final Mapping currentFullColumnIndex = rightInput.getFullRowType().fullColumnMapping;

            LabelUtil.multiplyAndShiftMap(newFullColumnIndexMap,
                newColumnInputSet,
                currentColumnIndex,
                currentFullColumnIndex,
                topShift,
                bottomShift);

            topShift += currentColumnIndex.getSourceCount();
            bottomShift += rightInput.getFullRowType().fullRowType.getFieldCount();
        } // end of for

        final Mapping newFullColumnMapping = LabelUtil
            .columnMapping(newFullColumnIndexMap.build(), topShift, bottomShift);

        return new FullRowType(newFullRowType, newFullColumnMapping, newColumnInputSet.build(), new HashMap<>());
    }

    /**
     * Build fullColumnMapping by concatenate fullColumnMapping from inputs
     */
    private static FullRowType concat(RelDataTypeFactory typeFactory, List<Label> inputs) {
        final ImmutableMap.Builder<Integer, Integer> newFullColumnIndexMap = ImmutableMap.builder();
        final ImmutableBitSet.Builder newColumnInputSet = ImmutableBitSet.builder();

        // build full row type
        final RelDataType newFullRowType = LabelUtil.concatRowTypes(inputs, typeFactory);

        // build fullColumnIndexMap & columnInputSet
        int topShift = 0;
        int bottomShift = 0;
        for (final Label rightInput : inputs) {
            final Mapping currentColumnIndex = rightInput.getColumnMapping();
            final Mapping currentFullColumnIndex = rightInput.getFullRowType().fullColumnMapping;

            LabelUtil.multiplyAndShiftMap(newFullColumnIndexMap,
                newColumnInputSet,
                currentColumnIndex,
                currentFullColumnIndex,
                topShift,
                bottomShift);

            topShift += currentColumnIndex.getSourceCount();
            bottomShift += rightInput.getFullRowType().fullRowType.getFieldCount();
        } // end of for

        final Mapping newFullColumnMapping = LabelUtil
            .columnMapping(newFullColumnIndexMap.build(), topShift, bottomShift);

        return new FullRowType(newFullRowType, newFullColumnMapping, newColumnInputSet.build(), new HashMap<>());
    }

    /**
     * Build identity mapping of rowType
     */
    private static FullRowType identity(RelDataType rowType) {
        final int fieldCount = rowType.getFieldCount();

        final Mapping columnMapping = LabelUtil.identityColumnMapping(fieldCount);

        return new FullRowType(rowType, columnMapping, ImmutableBitSet.range(fieldCount), new HashMap<>());
    }

    /**
     * Build fullColumnMapping by derive current fullColumnMapping from input
     */
    private static FullRowType derive(Label input) {
        final FullRowType bottom = input.getFullRowType();
        final Mapping fullColumnMapping = LabelUtil.multiply(input.getColumnMapping(), bottom.fullColumnMapping);

        final Builder builder = ImmutableBitSet.builder().addAll(() -> new Iterator<Integer>() {
            private final Iterator<IntPair> it = fullColumnMapping.iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Integer next() {
                return it.next().source;
            }
        });

        return new FullRowType(bottom.fullRowType,
            fullColumnMapping,
            builder.build(),
            input.getFullRowType().getColumnEqualities());
    }

    public Map<String, RexNode> getColumnEqualities() {
        return columnEqualities;
    }

    public Mapping getFullColumnMapping() {
        return fullColumnMapping;
    }

    public ImmutableBitSet getColumnInputSet() {
        return columnInputSet;
    }
}
