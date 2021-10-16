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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.exception.NotSupportException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * rift columns from the bottom scan to the top  rel.
 */
public class AddColumnsToRelNodeVisitor extends RelShuttleImpl {

    private RelNode root;
    private Map<RelNode, Map<Integer, Integer>> shiftMap;
    private Map<RelNode, Set<Integer>> addMap;
    private Map<RelNode, List<Integer>> childAddMap = Maps.newHashMap();
    private Map<RelNode, Map<Integer, Integer>> anchor;
    private boolean hasAgg;

    public AddColumnsToRelNodeVisitor(RelNode root, Map<RelNode, Set<Integer>> addMap,
                                      Map<RelNode, Map<Integer, Integer>> anchor) {
        this.root = root;
        this.shiftMap = Maps.newHashMap();
        this.addMap = addMap;
        this.anchor = anchor;
    }

    /**
     * if do not support(like union) ,return null.
     */
    public RelNode doJob() {
        RelNode r = root.accept(this);
        if (r instanceof LogicalAggregate || r instanceof LogicalJoin) {
            List<RexNode> pros = Lists.newLinkedList();
            List<String> fieldNames = Lists.newLinkedList();
            for (int j = 0; j < r.getRowType().getFieldCount(); j++) {
                pros.add(null);
                fieldNames.add(null);
            }
            Map<Integer, Integer> shift = shiftMap.get(r);
            for (Map.Entry<Integer, Integer> entry : shift.entrySet()) {
                pros.set(entry.getKey(),
                    new RexInputRef(entry.getValue(),
                        r.getRowType().getFieldList().get(entry.getValue()).getType()));
                fieldNames.set(entry.getKey(), r.getRowType().getFieldList().get(entry.getValue()).getName());
            }
            for (RelDataTypeField field : r.getRowType().getFieldList()) {
                for (int i = 0; ; i++) {
                    if (i >= r.getRowType().getFieldCount()) {
                        pros.add(new RexInputRef(field.getIndex(), field.getType()));
                        fieldNames.add(field.getName());
                        break;
                    } else if (pros.get(i) == null) {
                        pros.set(i, new RexInputRef(field.getIndex(), field.getType()));
                        fieldNames.set(i, field.getName());
                        break;
                    }
                }
            }

            LogicalProject lp = LogicalProject.create(r, pros, fieldNames);

            /**
             * fix anchor map
             */
            Map<Integer, Integer> anchorMap = anchor.get(r);
            Map<Integer, Integer> newAnchorMap = Maps.newHashMap();
            int count = 0;
            for (RexNode rex : lp.getProjects()) {
                RexInputRef rexInputRef = (RexInputRef) rex;
                if (anchorMap.containsKey(rexInputRef.getIndex())) {
                    int anchorKey = anchorMap.get(rexInputRef.getIndex());
                    newAnchorMap.put(count, anchorKey);
                }
                count++;
            }
            anchor.put(lp, newAnchorMap);
            return lp;
        }
        return r;
    }

    public Map<Integer, Integer> buildAnchor(RelNode root) {
        Map<Integer, Integer> m = anchor.get(root);
        Map<Integer, Integer> rs = Maps.newHashMap();
        if (m == null) {
            return rs;
        }
        for (Map.Entry<Integer, Integer> entry : m.entrySet()) {
            rs.put(entry.getValue(), entry.getKey());
        }
        return rs;
    }

    @Override
    public RelNode visit(LogicalProject project) {

        /**
         * handle child
         */
        RelNode child = project.getInput().accept(this);

        RelNode r = handleProject(project, child);
        return r;
    }

    private RelNode handleProject(LogicalProject project, RelNode child) {
        List<RexNode> pros = Lists.newLinkedList();

        Map<Integer, Integer> shift = shiftMap.get(child);
        List<Integer> childAdd = childAddMap.get(child);
        Set<Integer> toAdd = addMap.get(project);

        /**
         * shift old rexnode
         */
        if (shift != null) {
            for (RexNode oldPro : project.getProjects()) {
                RexNode newPro = RexUtil.shift(oldPro, shift);
                pros.add(newPro);
            }
        } else {
            pros.addAll(project.getProjects());
        }

        Set<Integer> addSet = Sets.newHashSet();
        Map<Integer, Integer> curAnchorMap = anchor.get(project);
        Map<Integer, Integer> childAnchorMap = anchor.get(child) == null ? Maps.newHashMap() : anchor.get(child);

        /**
         * shift to add
         */
        shiftToAdd(shift, childAdd, toAdd, addSet, curAnchorMap, childAnchorMap);

        /**
         * then, we add new input
         */
        List<String> newFieldNames = Lists.newArrayList();
        newFieldNames.addAll(project.getRowType().getFieldNames());

        List<Integer> addListForParent = Lists.newArrayList();
        Map<Integer, Integer> anchorMapForParent = Maps.newHashMap();
        for (Integer newIndex : addSet) {
            RelDataTypeField relDataTypeField = child.getRowType().getFieldList().get(newIndex);
            pros.add(new RexInputRef(newIndex, relDataTypeField.getType()));
            addListForParent.add(pros.size() - 1);
            anchorMapForParent.put(pros.size() - 1, childAnchorMap.get(newIndex));
            newFieldNames.add(relDataTypeField.getName());
        }

        RelNode r = project.copy(child, pros, newFieldNames);

        /**
         * ready the add to parent
         */
        childAddMap.put(r, addListForParent);
        anchor.put(r, anchorMapForParent);
        return r;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        /**
         * handler child
         */
        RelNode child = aggregate.getInput().accept(this);

        Map<Integer, Integer> shift = shiftMap.get(child);
        List<Integer> childAdd = childAddMap.get(child);
        Set<Integer> toAdd = addMap.get(aggregate);

        Set<Integer> addSet = Sets.newHashSet();
        Map<Integer, Integer> curAnchorMap = anchor.get(aggregate);
        Map<Integer, Integer> childAnchorMap = anchor.get(child);

        shiftToAdd(shift, childAdd, toAdd, addSet, curAnchorMap, childAnchorMap);

        /**
         * shift agg calls and group set
         */
        List<AggregateCall> newAggCalls = Lists.newArrayList();
        Set<Integer> newGroupSet = Sets.newHashSet();

        if (shift != null) {
            for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
                List<Integer> newArgs = Lists.newArrayList();
                for (Integer index : aggregateCall.getArgList()) {
                    if (shift.get(index) != null) {
                        newArgs.add(shift.get(index));
                    } else {
                        newArgs.add(index);
                    }
                }
                AggregateCall newAggregateCall = aggregateCall.copy(newArgs, aggregateCall.filterArg);
                newAggCalls.add(newAggregateCall);
            }

            // shift group sets
            List<Integer> oldGroupSet = aggregate.getGroupSet().asList();
            for (Integer o : oldGroupSet) {
                if (shift.get(o) != null) {
                    newGroupSet.add(shift.get(o));
                } else {
                    newGroupSet.add(o);
                }
            }

        } else {
            newAggCalls.addAll(aggregate.getAggCallList());
            newGroupSet.addAll(aggregate.getGroupSet().asList());
        }

        /**
         * add new group set
         * addSet should not overlap with new groupset
         */
        Map<Integer, Integer> shiftForParent = Maps.newHashMap();
        List<Integer> addSetForParent = Lists.newArrayList();
        newGroupSet.addAll(addSet);
        List<Integer> orderList = Lists.newArrayList(newGroupSet.iterator());
        Collections.sort(orderList);
        for (Integer a : addSet) {
            int index = orderList.indexOf(a);
            addSetForParent.add(index);

            Integer anchor = childAnchorMap.remove(a);
            childAnchorMap.put(index, anchor);

            /**
             * handle group set
             */
            for (int o = 0; o < aggregate.getGroupCount(); o++) {
                if (o >= index) {
                    if (shiftForParent.get(o) == null) {
                        shiftForParent.put(o, o + 1);
                    } else {
                        shiftForParent.put(o, shiftForParent.get(o) + 1);
                    }
                }
            }
        }
        for (int n = 0; n < aggregate.getAggCallList().size(); n++) {
            int originalIndex = aggregate.getGroupCount() + n;
            int newIndex = originalIndex + addSet.size();
            shiftForParent.put(originalIndex, newIndex);
        }

        RelNode r = aggregate.copy(child, ImmutableBitSet.of(newGroupSet), newAggCalls);

        if (newAggCalls.size() > 0) {
            hasAgg = true;
        }

        shiftMap.put(r, shiftForParent);
        childAddMap.put(r, addSetForParent);
        anchor.put(r, childAnchorMap);
        return r;
    }

    protected void shiftToAdd(Map<Integer, Integer> shift, List<Integer> childAdd, Set<Integer> toAdd,
                              Set<Integer> addSet, Map<Integer, Integer> curAnchorMap,
                              Map<Integer, Integer> childAnchorMap) {
        /**
         * shift to add
         */
        if (childAdd != null) {
            addSet.addAll(childAdd);
        }
        if (shift != null) {
            if (toAdd != null) {
                addSet.addAll(shiftList(toAdd, shift));
            }
            if (curAnchorMap != null) {
                childAnchorMap.putAll(shiftAnchorMap(curAnchorMap, shift));
            }
        } else {
            if (toAdd != null) {
                addSet.addAll(toAdd);
            }
            if (curAnchorMap != null) {
                childAnchorMap.putAll(curAnchorMap);
            }
        }
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        /**
         * handler child
         */
        RelNode child = filter.getInput().accept(this);

        Map<Integer, Integer> shift = shiftMap.get(child);
        List<Integer> childAdd = childAddMap.get(child);
        Map<Integer, Integer> anchorMap = anchor.get(child);

        /**
         * shift
         */
        RexNode condition = filter.getCondition();

        if (shift != null) {
            condition = RexUtil.shift(condition, shift);
        }

        /**
         * keep delivery addset and shiftmap to its parents.
         */
        RelNode r = filter.copy(filter.getTraitSet(), child, condition);

        shiftMap.put(r, shift);
        childAddMap.put(r, childAdd);
        anchor.put(r, anchorMap);
        return r;
    }

    /**
     * sort donot add
     */
    @Override
    public RelNode visit(LogicalSort sort) {
        RelNode child = sort.getInput().accept(this);

        Map<Integer, Integer> shift = shiftMap.get(child);
        List<Integer> childAdd = childAddMap.get(child);
        Map<Integer, Integer> anchorMap = anchor.get(child);

        RelNode r;
        if (shift == null || shift.size() == 0) {
            r = sort.copy(child, sort.getCollation());
        } else {

            /**
             * handler child
             */
            RelCollation relCollation = sort.getCollation();
            List<RelFieldCollation> newRelCollationList = Lists.newArrayList();
            for (RelFieldCollation relFieldCollation : relCollation.getFieldCollations()) {
                int index = relFieldCollation.getFieldIndex();
                int shiftIndex = shift.get(index);
                RelFieldCollation newRelField = relFieldCollation.copy(shiftIndex);
                newRelCollationList.add(newRelField);
            }

            r = sort.copy(child, RelCollations.of(newRelCollationList));
        }

        shiftMap.put(r, shift);
        childAddMap.put(r, childAdd);
        anchor.put(r, anchorMap);
        return r;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        /**
         * handler child
         */
        RelNode leftChild = join.getLeft().accept(this);
        RelNode rightChild = join.getRight().accept(this);

        List<Integer> leftChildAdd = childAddMap.get(leftChild);
        leftChildAdd = leftChildAdd == null ? Lists.newArrayList() : leftChildAdd;
        List<Integer> rightChildAdd = childAddMap.get(rightChild);
        rightChildAdd = rightChildAdd == null ? Lists.newArrayList() : rightChildAdd;

        Map<Integer, Integer> leftChildShift = shiftMap.get(leftChild);
        leftChildShift = leftChildShift == null ? Maps.newHashMap() : leftChildShift;
        Map<Integer, Integer> rightChildShift = shiftMap.get(rightChild);
        rightChildShift = rightChildShift == null ? Maps.newHashMap() : rightChildShift;

        Map<Integer, Integer> leftAnchorMap = anchor.get(leftChild);
        leftAnchorMap = leftAnchorMap == null ? Maps.newHashMap() : leftAnchorMap;
        Map<Integer, Integer> rightAnchorMap = anchor.get(rightChild);
        rightAnchorMap = rightAnchorMap == null ? Maps.newHashMap() : rightAnchorMap;

        /**
         * join shift map
         */
        int leftSize = leftChild.getRowType().getFieldCount();
        rightChildShift = shiftMapAll(rightChildShift, leftSize);

        leftChildShift.putAll(rightChildShift);

        /**
         * shift right add list
         */
        rightChildAdd = shiftListAll(rightChildAdd, leftSize);

        leftChildAdd.addAll(rightChildAdd);

        /**
         * shift right anchor map
         */
        rightAnchorMap = shiftMapAllForAnchor(rightAnchorMap, leftSize);
        leftAnchorMap.putAll(rightAnchorMap);

        RelNode r = join.copy(join.getTraitSet(),
            RexUtil.shift(join.getCondition(), leftChildShift),
            leftChild,
            rightChild,
            join.getJoinType(),
            join.isSemiJoinDone());
        shiftMap.put(r, leftChildShift);
        childAddMap.put(r, leftChildAdd);
        anchor.put(r, leftAnchorMap);
        return r;
    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof Gather) {
            RelNode child = other.getInput(0);
            RelNode newChild = visit(child);
            RelNode r = Gather.create(newChild);

            /**
             * delivery add and shift
             */
            childAddMap.put(r, childAddMap.get(newChild));
            shiftMap.put(r, shiftMap.get(newChild));
            anchor.put(r, anchor.get(newChild));
            return r;
        }
        return visitChildren(other);
    }

    private List<Integer> shiftListAll(List<Integer> list, int size) {
        List<Integer> rs = Lists.newArrayList();
        for (Integer i : list) {
            rs.add(i + size);
        }
        return rs;
    }

    private Map<Integer, Integer> shiftMapAll(Map<Integer, Integer> map, int size) {
        Map<Integer, Integer> rs = Maps.newHashMap();
        for (Map.Entry<Integer, Integer> e : map.entrySet()) {
            rs.put(e.getKey() + size, e.getValue() + size);
        }
        return rs;
    }

    private Map<Integer, Integer> shiftMapAllForAnchor(Map<Integer, Integer> map, int size) {
        Map<Integer, Integer> rs = Maps.newHashMap();
        for (Map.Entry<Integer, Integer> e : map.entrySet()) {
            rs.put(e.getKey() + size, e.getValue());
        }
        return rs;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        throw new NotSupportException("union with correlate subquery");
    }

    private List<Integer> shiftList(Set<Integer> l, Map<Integer, Integer> addShift) {
        List<Integer> list = Lists.newArrayList();
        for (Integer i : l) {
            if (addShift.get(i) != null) {
                list.add(addShift.get(i));
            } else {
                list.add(i);
            }
        }
        return list;
    }

    private Map<? extends Integer, ? extends Integer> shiftAnchorMap(Map<Integer, Integer> anchorMap,
                                                                     Map<Integer, Integer> shift) {
        Map<Integer, Integer> map = Maps.newHashMap();
        for (Map.Entry<Integer, Integer> p : anchorMap.entrySet()) {
            if (shift.get(p.getKey()) != null) {
                map.put(shift.get(p.getKey()), p.getValue());
            } else {
                map.put(p.getKey(), p.getValue());
            }
        }
        return map;
    }

    private void remarkAnchorForParent(Map<Integer, Integer> anchorMarkMap, Integer index, int replace) {
        List<Integer> it = Lists.newArrayList(anchorMarkMap.keySet());
        for (Integer i : it) {
            if (i == index) {
                int value = anchorMarkMap.get(i);
                anchorMarkMap.remove(i);
                anchorMarkMap.put(replace, value);
                return;
            }
        }
    }

    public boolean isHasAgg() {
        return hasAgg;
    }
}
