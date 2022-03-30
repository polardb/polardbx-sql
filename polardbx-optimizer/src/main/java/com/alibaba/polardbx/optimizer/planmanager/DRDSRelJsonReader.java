/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeAND;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.common.model.sqljep.ExtComparative;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.GroupConcatAggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reads a JSON plan and converts it back to a tree of relational expressions.
 *
 * @see RelInput
 */
public class DRDSRelJsonReader {

    private static final TypeReference<HashMap<String, Object>> TYPE_REF =
        new TypeReference<HashMap<String, Object>>() {
        };

    private final RelOptCluster cluster;
    private final RelOptSchema relOptSchema;
    protected DRDSRelJson relJson;
    private final Map<String, RelNode> relMap = new LinkedHashMap<>();
    private RelNode lastRel;

    private boolean supportMpp;

    public DRDSRelJsonReader(RelOptCluster cluster, RelOptSchema relOptSchema,
                             Schema schema, boolean supportMpp) {
        this.cluster = cluster;
        this.relOptSchema = relOptSchema;
        this.supportMpp = supportMpp;
        Util.discard(schema);
        relJson = new DRDSRelJson(null, supportMpp);
    }

    public RelNode read(String s) throws IOException {
        lastRel = null;
        Map<String, Object> o;
        o = JSON.parseObject(s, TYPE_REF.getType());
        @SuppressWarnings("unchecked") final List<Map<String, Object>> rels = (List) o.get("rels");
        readRels(rels);
        return lastRel;
    }

    public RelNode read(List<Map<String, Object>> jsonRels) throws IOException {
        lastRel = null;
        readRels(jsonRels);
        return lastRel;
    }

    public List<Map<String, Comparative>> readComparatives(List<Map<String, Object>> comparatives) throws IOException {
        List<Map<String, Comparative>> list = new ArrayList<>();
        for (Map<String, Object> inferred : comparatives) {
            Map<String, Comparative> m = new HashMap<>();
            for (String key : inferred.keySet()) {
                Map<String, Object> value = (Map<String, Object>) inferred.get(key);
                if (value != null) {
                    m.put(key, readComparative(value));
                } else {
                    m.put(key, null);
                }
            }
            list.add(m);
        }
        return list;
    }

    private Comparative readComparative(Map<String, Object> m) {
        Comparative comparative;
        String type = (String) m.get("type");
        if (type.equals(ComparativeAND.class.getSimpleName())) {
            List<Map<String, Object>> list = (List<Map<String, Object>>) m.get("list");
            comparative = new ComparativeAND();
            for (Map<String, Object> mm : list) {
                ((ComparativeAND) comparative).addComparative(readComparative(mm));
            }
        } else if (type.equals(ComparativeOR.class.getSimpleName())) {
            List<Map<String, Object>> list = (List<Map<String, Object>>) m.get("list");
            comparative = new ComparativeOR();
            for (Map<String, Object> mm : list) {
                ((ComparativeOR) comparative).addComparative(readComparative(mm));
            }
        } else if (type.equals(ExtComparative.class.getSimpleName())) {
            String columnName = (String) m.get("columnName");
            Integer comparison = (Integer) m.get("comparison");
            Object value = m.get("value");
            RexNode rexNode = relJson.toRex(makeEmptyRelInput(), value);
            comparative = new ExtComparative(columnName, comparison, rexNode);
        } else {
            Integer comparison = (Integer) m.get("comparison");
            Object value = m.get("value");
            RexNode rexNode = relJson.toRex(makeEmptyRelInput(), value);
            comparative = new Comparative(comparison, rexNode);
        }
        return comparative;
    }

    private void readRels(List<Map<String, Object>> jsonRels) {
        for (Map<String, Object> jsonRel : jsonRels) {
            readRel(jsonRel);
        }
    }

    private void readRel(final Map<String, Object> jsonRel) {
        String id = (String) jsonRel.get("id");
        String type = (String) jsonRel.get("relOp");
        Integer relatedId = null;
        if (jsonRel.get("relatedId") instanceof Integer) {
            relatedId = (Integer) jsonRel.get("relatedId");
            ;
        } else {
            relatedId = null;
        }
        Constructor constructor = relJson.getConstructor(type);
        RelInput input = new RelInput() {
            public RelOptCluster getCluster() {
                return cluster;
            }

            public RelTraitSet getTraitSet() {
                return cluster.traitSetOf(Convention.NONE);
            }

            @Override
            public boolean supportMpp() {
                return supportMpp;
            }

            @Override
            public List<Window.Group> getWindowGroups() {
                RelNode input = this.getInput();
                JSONArray groups = (JSONArray) this.get("groups");
                List<Window.Group> windowGroups = new ArrayList(groups.size());
                for (int i = 0; i < groups.size(); i++) {
                    JSONObject jsonGroup = (JSONObject) groups.get(i);
                    RexWindowBound upperRexWindowBound =
                        getRexWindowBound(jsonGroup.getJSONObject("upperBound"), SqlWindow.FOLLOWING_OPERATOR);
                    RexWindowBound lowerRexWindowBound =
                        getRexWindowBound(jsonGroup.getJSONObject("lowerBound"), SqlWindow.PRECEDING_OPERATOR);

                    RelCollation collation = RelCollations.EMPTY;
                    JSONArray orderKeys = jsonGroup.getJSONArray("orderKeys");
                    if (orderKeys != null) {
                        List<Map<String, Object>> mapList = handleJSONArray(orderKeys);
                        collation = relJson.toCollation(mapList);
                    }
                    ImmutableBitSet keys = ImmutableBitSet
                        .of(jsonGroup.getJSONArray("keys").stream().map(t -> ((Integer) t).intValue()).collect(
                            Collectors.toList()));
                    boolean isRows = jsonGroup.getBooleanValue("isRows");
                    List<Window.RexWinAggCall> rexWinAggCalls =
                        getWindowAggregateCalls(jsonGroup.getJSONArray("calls"));
                    Window.Group group = new Window.Group(keys,
                        isRows,
                        lowerRexWindowBound,
                        upperRexWindowBound,
                        collation,
                        rexWinAggCalls);
                    windowGroups.add(group);
                }
                return windowGroups;
            }

            private RexWindowBound getRexWindowBound(JSONObject upperBound, SqlPostfixOperator operator) {
                if (upperBound == null) {
                    return null;
                }
                Boolean unbounded = upperBound.getBoolean("unbounded");
                Boolean currentRow = upperBound.getBoolean("currentRow");
                Boolean following = upperBound.getBoolean("following");
                Boolean preceding = upperBound.getBoolean("preceding");
                JSONObject offset = upperBound.getJSONObject("offset");
                RexNode rexNode = relJson.toRex(this, offset);
                RexWindowBound rexBound = null;
                if (rexNode != null) {
                    List<RexNode> operands = Lists.newArrayList(rexNode);
                    rexBound =
                        RexWindowBound
                            .create(unbounded, currentRow, following, preceding, operands, operator, rexNode.getType());
                } else {
                    rexBound =
                        RexWindowBound
                            .create(unbounded, currentRow, following, preceding, null, null, null);
                }
                return rexBound;

            }

            private Window.RexWinAggCall toWindowAggCall(Map<String, Object> jsonAggCall) {
                final String aggName = (String) jsonAggCall.get("agg");
                final SqlAggFunction aggregation =
                    relJson.toAggregation(aggName, jsonAggCall);
                final Boolean distinct = (Boolean) jsonAggCall.get("distinct");
                @SuppressWarnings("unchecked") final List<Map<String, Object>> args =
                    (List<Map<String, Object>>) jsonAggCall.get("operands");
                List<RexNode> operands = Lists.newArrayList();
                for (Map<String, Object> operand : args) {
                    int index = Integer.valueOf(operand.get("input").toString());
                    operands.add(new RexInputRef(index, getInput().getRowType().getFieldList().get(index).getType()));
                }

                final RelDataType type =
                    relJson.toType(cluster.getTypeFactory(), jsonAggCall.get("type"));
                //ordinal must not be null
                Integer ordinal = Integer.valueOf(jsonAggCall.get("ordinal").toString());
                Window.RexWinAggCall rexWinAggCall = new Window.RexWinAggCall(aggregation,
                    type,
                    operands,
                    ordinal,
                    false);
                return rexWinAggCall;
            }

            public RelOptTable getTable(String table) {
                final List<String> list = getStringList(table);
                return relOptSchema.getTableForMember(list);
            }

            public RelNode getInput() {
                final List<RelNode> inputs = getInputs();
                assert inputs.size() == 1;
                return inputs.get(0);
            }

            public List<RelNode> getInputs() {
                final List<String> jsonInputs = getStringList("inputs");
                if (jsonInputs == null) {
                    return ImmutableList.of(lastRel);
                }
                final List<RelNode> inputs = new ArrayList<>();
                for (String jsonInput : jsonInputs) {
                    inputs.add(lookupInput(jsonInput));
                }
                return inputs;
            }

            public RexNode getExpression(String tag) {
                return relJson.toRex(this, jsonRel.get(tag));
            }

            public SqlOperator getSqlOperator(String tag) {
                return relJson.toOp((String) jsonRel.get(tag), new HashMap<>());
            }

            public ImmutableBitSet getBitSet(String tag) {
                return ImmutableBitSet.of(getIntegerList(tag));
            }

            public List<ImmutableBitSet> getBitSetList(String tag) {
                List<List<Integer>> list = getIntegerListList(tag);
                if (list == null) {
                    return null;
                }
                final ImmutableList.Builder<ImmutableBitSet> builder =
                    ImmutableList.builder();
                for (List<Integer> integers : list) {
                    builder.add(ImmutableBitSet.of(integers));
                }
                return builder.build();
            }

            public List<String> getStringList(String tag) {
                //noinspection unchecked
                return (List<String>) jsonRel.get(tag);
            }

            public List<Integer> getIntegerList(String tag) {
                //noinspection unchecked
                return (List<Integer>) jsonRel.get(tag);
            }

            public List<List<Integer>> getIntegerListList(String tag) {
                //noinspection unchecked
                return (List<List<Integer>>) jsonRel.get(tag);
            }

            public List<AggregateCall> getAggregateCalls(String tag) {
                @SuppressWarnings("unchecked") final List<Map<String, Object>> jsonAggs = (List) jsonRel.get(tag);
                final List<AggregateCall> inputs = new ArrayList<>();
                for (Map<String, Object> jsonAggCall : jsonAggs) {
                    inputs.add(toAggCall(jsonAggCall));
                }
                return inputs;
            }

            public List<Window.RexWinAggCall> getWindowAggregateCalls(JSONArray jsonCalls) {
                final List<Window.RexWinAggCall> inputs = new ArrayList<>();
                for (Object jsonAggCall : jsonCalls) {
                    inputs.add(toWindowAggCall((Map<String, Object>) jsonAggCall));
                }
                return inputs;
            }

            public Object get(String tag) {
                return jsonRel.get(tag);
            }

            public String getString(String tag) {
                return (String) jsonRel.get(tag);
            }

            public float getFloat(String tag) {
                return ((Number) jsonRel.get(tag)).floatValue();
            }

            public int getInteger(String tag) {
                return ((Number) jsonRel.get(tag)).intValue();
            }

            public boolean getBoolean(String tag, boolean default_) {
                final Boolean b = (Boolean) jsonRel.get(tag);
                return b != null ? b : default_;
            }

            public <E extends Enum<E>> E getEnum(String tag, Class<E> enumClass) {
                return Util.enumVal(enumClass,
                    getString(tag).toUpperCase(Locale.ROOT));
            }

            public List<RexNode> getExpressionList(String tag) {
                @SuppressWarnings("unchecked") final List<Object> jsonNodes = (List) jsonRel.get(tag);
                final List<RexNode> nodes = new ArrayList<>();
                for (Object jsonNode : jsonNodes) {
                    nodes.add(relJson.toRex(this, jsonNode));
                }
                return nodes;
            }

            public List<List<RexNode>> getExpressionListList(String tag) {
                @SuppressWarnings("unchecked") final List<List<Object>> jsonNodeListList = (List) jsonRel.get(tag);
                final List<List<RexNode>> nodeListList = new ArrayList<>();
                for (List<Object> jsonNodeList : jsonNodeListList) {
                    List<RexNode> nodeList = new ArrayList<>();
                    for (Object jsonNode : jsonNodeList) {
                        nodeList.add(relJson.toRex(this, jsonNode));
                    }
                    nodeListList.add(nodeList);
                }
                return nodeListList;
            }

            public RelDataType getRowType(String tag) {
                final Object o = jsonRel.get(tag);
                return relJson.toType(cluster.getTypeFactory(), o);
            }

            public RelDataType getRowType(String expressionsTag, String fieldsTag) {
                final List<RexNode> expressionList = getExpressionList(expressionsTag);
                @SuppressWarnings("unchecked") final List<String> names =
                    (List<String>) get(fieldsTag);
                return cluster.getTypeFactory().createStructType(
                    new AbstractList<Map.Entry<String, RelDataType>>() {
                        @Override
                        public Map.Entry<String, RelDataType> get(int index) {
                            return Pair.of(names.get(index),
                                expressionList.get(index).getType());
                        }

                        @Override
                        public int size() {
                            return names.size();
                        }
                    });
            }

            public RelCollation getCollation() {
                //noinspection unchecked
                return relJson.toCollation((List) get("collation"));
            }

            public RelDistribution getDistribution() {
                return relJson.toDistribution((Map<String, Object>) get("distribution"));
            }

            public ImmutableList<ImmutableList<RexLiteral>> getTuples(String tag) {
                //noinspection unchecked
                final List<List> jsonTuples = (List) get(tag);
                final ImmutableList.Builder<ImmutableList<RexLiteral>> builder =
                    ImmutableList.builder();
                for (List jsonTuple : jsonTuples) {
                    builder.add(getTuple(jsonTuple));
                }
                return builder.build();
            }

            public ImmutableList<RexLiteral> getTuple(List jsonTuple) {
                final ImmutableList.Builder<RexLiteral> builder =
                    ImmutableList.builder();
                for (Object jsonValue : jsonTuple) {
                    builder.add((RexLiteral) relJson.toRex(this, jsonValue));
                }
                return builder.build();
            }

            public ImmutableList<ImmutableList<RexNode>> getDynamicTuples(String tag) {
                //noinspection unchecked
                final List<List> jsonTuples = (List) get(tag);
                final ImmutableList.Builder<ImmutableList<RexNode>> builder =
                    ImmutableList.builder();
                for (List jsonTuple : jsonTuples) {
                    builder.add(getDynamicTuple(jsonTuple));
                }
                return builder.build();
            }

            private ImmutableList<RexNode> getDynamicTuple(List jsonTuple) {
                final ImmutableList.Builder<RexNode> builder =
                    ImmutableList.builder();
                for (Object jsonValue : jsonTuple) {
                    builder.add(relJson.toRex(this, jsonValue));
                }
                return builder.build();
            }

            public ImmutableSet<CorrelationId> getVariablesSet() {
                if (jsonRel.get("variablesSet") != null) {
                    Set<CorrelationId> correlationIdSet = new HashSet<>();
                    for (Object id : (List)jsonRel.get("variablesSet")) {
                        correlationIdSet.add(new CorrelationId(((Number)id).intValue()));
                    }
                    return ImmutableSet.copyOf(correlationIdSet);
                } else {
                    return ImmutableSet.of();
                }
            }
        };
        try {
            final RelNode rel = (RelNode) constructor.newInstance(input);
            if (relatedId != null) {
                rel.setRelatedId(relatedId);
            }
            relMap.put(id, rel);
            lastRel = rel;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            final Throwable e2 = e.getCause();
            if (e2 instanceof RuntimeException) {
                throw (RuntimeException) e2;
            }
            throw new RuntimeException(e2);
        }
    }

    private AggregateCall toAggCall(Map<String, Object> jsonAggCall) {
        final String aggName = (String) jsonAggCall.get("agg");
        final SqlAggFunction aggregation =
            relJson.toAggregation(aggName, jsonAggCall);
        final Boolean distinct = (Boolean) jsonAggCall.get("distinct");
        @SuppressWarnings("unchecked") final List<Integer> operands = (List<Integer>) jsonAggCall.get("operands");
        final Integer filterOperand = (Integer) jsonAggCall.get("filter");
        final RelDataType type =
            relJson.toType(cluster.getTypeFactory(), jsonAggCall.get("type"));
        if (jsonAggCall.get("isGroupConcat") != null) {
            final List<Integer> orderList = (List<Integer>) jsonAggCall.get("orderList");
            final List<String> ascOrDescList = (List<String>) jsonAggCall.get("ascOrDescList");
            final String separator = (String) jsonAggCall.get("separator");
            return GroupConcatAggregateCall.create(aggregation, distinct, false, operands,
                filterOperand == null ? -1 : filterOperand, type, null, orderList, separator, ascOrDescList);
        } else {
            return AggregateCall.create(aggregation, distinct, false, operands,
                filterOperand == null ? -1 : filterOperand, type, null);
        }
    }

    private RelNode lookupInput(String jsonInput) {
        RelNode node = relMap.get(jsonInput);
        if (node == null) {
            throw new RuntimeException("unknown id " + jsonInput
                + " for relational expression");
        }
        return node;
    }

    private RelInput makeEmptyRelInput() {
        Map<String, Object> jsonRel = new HashMap<>();
        return new RelInput() {
            public RelOptCluster getCluster() {
                return cluster;
            }

            public RelTraitSet getTraitSet() {
                return cluster.traitSetOf(Convention.NONE);
            }

            @Override
            public boolean supportMpp() {
                return supportMpp;
            }

            @Override
            public List<Window.Group> getWindowGroups() {
                ImmutableBitSet keys = this.getBitSet("keys");
                List<AggregateCall> calls = this.getAggregateCalls("calls");
                List<Window.RexWinAggCall> rexWinAggCalls = Lists.newLinkedList();
                RelNode input = this.getInput();
                for (AggregateCall aggregateCall : calls) {
                    List<RexNode> args = Lists.newArrayList();
                    for (Integer index : aggregateCall.getArgList()) {
                        args.add(new RexInputRef(index, input.getRowType().getFieldList().get(index).getType()));
                    }
                    Window.RexWinAggCall rexWinAggCall = new Window.RexWinAggCall(aggregateCall.getAggregation(),
                        aggregateCall.getType(),
                        args,
                        0,
                        false);
                    rexWinAggCalls.add(rexWinAggCall);
                }
                Window.Group newGroup = new Window.Group(keys,
                    false,
                    RexWindowBound.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null),
                    RexWindowBound.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null),
                    RelCollations.EMPTY,
                    rexWinAggCalls);

                // only support 1 group
                return Lists.newArrayList(newGroup);
            }

            public RelOptTable getTable(String table) {
                final List<String> list = getStringList(table);
                return relOptSchema.getTableForMember(list);
            }

            public RelNode getInput() {
                final List<RelNode> inputs = getInputs();
                assert inputs.size() == 1;
                return inputs.get(0);
            }

            public List<RelNode> getInputs() {
                final List<String> jsonInputs = getStringList("inputs");
                if (jsonInputs == null) {
                    return ImmutableList.of(lastRel);
                }
                final List<RelNode> inputs = new ArrayList<>();
                for (String jsonInput : jsonInputs) {
                    inputs.add(lookupInput(jsonInput));
                }
                return inputs;
            }

            public RexNode getExpression(String tag) {
                return relJson.toRex(this, jsonRel.get(tag));
            }

            public SqlOperator getSqlOperator(String tag) {
                return relJson.toOp(tag, new HashMap<>());
            }

            public ImmutableBitSet getBitSet(String tag) {
                return ImmutableBitSet.of(getIntegerList(tag));
            }

            public List<ImmutableBitSet> getBitSetList(String tag) {
                List<List<Integer>> list = getIntegerListList(tag);
                if (list == null) {
                    return null;
                }
                final ImmutableList.Builder<ImmutableBitSet> builder =
                    ImmutableList.builder();
                for (List<Integer> integers : list) {
                    builder.add(ImmutableBitSet.of(integers));
                }
                return builder.build();
            }

            public List<String> getStringList(String tag) {
                //noinspection unchecked
                return (List<String>) jsonRel.get(tag);
            }

            public List<Integer> getIntegerList(String tag) {
                //noinspection unchecked
                return (List<Integer>) jsonRel.get(tag);
            }

            public List<List<Integer>> getIntegerListList(String tag) {
                //noinspection unchecked
                return (List<List<Integer>>) jsonRel.get(tag);
            }

            public List<AggregateCall> getAggregateCalls(String tag) {
                @SuppressWarnings("unchecked") final List<Map<String, Object>> jsonAggs = (List) jsonRel.get(tag);
                final List<AggregateCall> inputs = new ArrayList<>();
                for (Map<String, Object> jsonAggCall : jsonAggs) {
                    inputs.add(toAggCall(jsonAggCall));
                }
                return inputs;
            }

            public Object get(String tag) {
                return jsonRel.get(tag);
            }

            public String getString(String tag) {
                return (String) jsonRel.get(tag);
            }

            public float getFloat(String tag) {
                return ((Number) jsonRel.get(tag)).floatValue();
            }

            public int getInteger(String tag) {
                return ((Number) jsonRel.get(tag)).intValue();
            }

            public boolean getBoolean(String tag, boolean default_) {
                final Boolean b = (Boolean) jsonRel.get(tag);
                return b != null ? b : default_;
            }

            public <E extends Enum<E>> E getEnum(String tag, Class<E> enumClass) {
                return Util.enumVal(enumClass,
                    getString(tag).toUpperCase(Locale.ROOT));
            }

            public List<RexNode> getExpressionList(String tag) {
                @SuppressWarnings("unchecked") final List<Object> jsonNodes = (List) jsonRel.get(tag);
                final List<RexNode> nodes = new ArrayList<>();
                for (Object jsonNode : jsonNodes) {
                    nodes.add(relJson.toRex(this, jsonNode));
                }
                return nodes;
            }

            public List<List<RexNode>> getExpressionListList(String tag) {
                @SuppressWarnings("unchecked") final List<List<Object>> jsonNodeListList = (List) jsonRel.get(tag);
                final List<List<RexNode>> nodeListList = new ArrayList<>();
                for (List<Object> jsonNodeList : jsonNodeListList) {
                    List<RexNode> nodeList = new ArrayList<>();
                    for (Object jsonNode : jsonNodeList) {
                        nodeList.add(relJson.toRex(this, jsonNode));
                    }
                    nodeListList.add(nodeList);
                }
                return nodeListList;
            }

            public RelDataType getRowType(String tag) {
                final Object o = jsonRel.get(tag);
                return relJson.toType(cluster.getTypeFactory(), o);
            }

            public RelDataType getRowType(String expressionsTag, String fieldsTag) {
                final List<RexNode> expressionList = getExpressionList(expressionsTag);
                @SuppressWarnings("unchecked") final List<String> names =
                    (List<String>) get(fieldsTag);
                return cluster.getTypeFactory().createStructType(
                    new AbstractList<Map.Entry<String, RelDataType>>() {
                        @Override
                        public Map.Entry<String, RelDataType> get(int index) {
                            return Pair.of(names.get(index),
                                expressionList.get(index).getType());
                        }

                        @Override
                        public int size() {
                            return names.size();
                        }
                    });
            }

            public RelCollation getCollation() {
                //noinspection unchecked
                return relJson.toCollation((List) get("collation"));
            }

            public RelDistribution getDistribution() {
                return relJson.toDistribution((Map<String, Object>) get("distribution"));
            }

            public ImmutableList<ImmutableList<RexLiteral>> getTuples(String tag) {
                //noinspection unchecked
                final List<List> jsonTuples = (List) get(tag);
                final ImmutableList.Builder<ImmutableList<RexLiteral>> builder =
                    ImmutableList.builder();
                for (List jsonTuple : jsonTuples) {
                    builder.add(getTuple(jsonTuple));
                }
                return builder.build();
            }

            public ImmutableList<RexLiteral> getTuple(List jsonTuple) {
                final ImmutableList.Builder<RexLiteral> builder =
                    ImmutableList.builder();
                for (Object jsonValue : jsonTuple) {
                    builder.add((RexLiteral) relJson.toRex(this, jsonValue));
                }
                return builder.build();
            }

            public ImmutableList<ImmutableList<RexNode>> getDynamicTuples(String tag) {
                //noinspection unchecked
                final List<List> jsonTuples = (List) get(tag);
                final ImmutableList.Builder<ImmutableList<RexNode>> builder =
                    ImmutableList.builder();
                for (List jsonTuple : jsonTuples) {
                    builder.add(getDynamicTuple(jsonTuple));
                }
                return builder.build();
            }

            private ImmutableList<RexNode> getDynamicTuple(List jsonTuple) {
                final ImmutableList.Builder<RexNode> builder =
                    ImmutableList.builder();
                for (Object jsonValue : jsonTuple) {
                    builder.add((RexDynamicParam) relJson.toRex(this, jsonValue));
                }
                return builder.build();
            }

            public ImmutableSet<CorrelationId> getVariablesSet() {
                if (jsonRel.get("variablesSet") != null) {
                    Set<CorrelationId> correlationIdSet = new HashSet<>();
                    for (Object id : (List)jsonRel.get("variablesSet")) {
                        correlationIdSet.add(new CorrelationId(((Number)id).intValue()));
                    }
                    return ImmutableSet.copyOf(correlationIdSet);
                } else {
                    return ImmutableSet.of();
                }
            }
        };
    }

    private Map<String, Object> fromJson2Map(String jsonString) {
        HashMap jsonMap = JSON.parseObject(jsonString, HashMap.class);

        HashMap<String, Object> resultMap = new HashMap<String, Object>();
        for (Iterator iter = jsonMap.keySet().iterator(); iter.hasNext(); ) {
            String key = (String) iter.next();
            if (jsonMap.get(key) instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray) jsonMap.get(key);
                List list = handleJSONArray(jsonArray);
                resultMap.put(key, list);
            } else {
                resultMap.put(key, jsonMap.get(key));
            }
        }
        return resultMap;
    }

    private List<Map<String, Object>> handleJSONArray(JSONArray jsonArray) {
        List list = new ArrayList();
        for (Object object : jsonArray) {
            JSONObject jsonObject = (JSONObject) object;
            HashMap map = new HashMap<String, Object>();
            for (Map.Entry entry : jsonObject.entrySet()) {
                if (entry.getValue() instanceof JSONArray) {
                    map.put((String) entry.getKey(), handleJSONArray((JSONArray) entry.getValue()));
                } else {
                    map.put((String) entry.getKey(), entry.getValue());
                }
            }
            list.add(map);
        }
        return list;
    }

}

// End RelJsonReader.java
