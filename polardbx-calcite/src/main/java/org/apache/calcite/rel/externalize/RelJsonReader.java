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
package org.apache.calcite.rel.externalize;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Reads a JSON plan and converts it back to a tree of relational expressions.
 *
 * @see org.apache.calcite.rel.RelInput
 */
public class RelJsonReader {
  private static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF =
      new TypeReference<LinkedHashMap<String, Object>>() {
      };

  private final RelOptCluster cluster;
  private final RelOptSchema relOptSchema;
  protected RelJson relJson = new RelJson(null);
  private final Map<String, RelNode> relMap = new LinkedHashMap<>();
  private RelNode lastRel;

  public RelJsonReader(RelOptCluster cluster, RelOptSchema relOptSchema,
      Schema schema) {
    this.cluster = cluster;
    this.relOptSchema = relOptSchema;
    Util.discard(schema);
  }

  public RelNode read(String s) throws IOException {
    lastRel = null;
    final ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> o = mapper.readValue(s, TYPE_REF);
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> rels = (List) o.get("rels");
    readRels(rels);
    return lastRel;
  }

  public RelNode read(List<Map<String, Object>> jsonRels) throws IOException {
    lastRel = null;
    readRels(jsonRels);
    return lastRel;
  }

  private void readRels(List<Map<String, Object>> jsonRels) {
    for (Map<String, Object> jsonRel : jsonRels) {
      readRel(jsonRel);
    }
  }

  private void readRel(final Map<String, Object> jsonRel) {
    String id = (String) jsonRel.get("id");
    String type = (String) jsonRel.get("relOp");
    Constructor constructor = relJson.getConstructor(type);
    RelJsonReader that = this;
    RelInput input = new RelInput() {
      public RelOptCluster getCluster() {
        return cluster;
      }

      @Override
      public boolean supportMpp() {
        return false;
      }

      @Override public List<Window.Group> getWindowGroups() {
        ImmutableBitSet keys = this.getBitSet("keys");
        List<AggregateCall> calls = this.getAggregateCalls("calls");
        List<Window.RexWinAggCall> rexWinAggCalls = Lists.newLinkedList();
        RelNode input = this.getInput();
        for(AggregateCall aggregateCall:calls){
          List<RexNode> args = Lists.newArrayList();
          for(Integer index:aggregateCall.getArgList()){
            args.add(new RexInputRef(index, input.getRowType().getFieldList().get(index).getType()));
          }
          Window.RexWinAggCall rexWinAggCall = new Window.RexWinAggCall(aggregateCall.getAggregation(), aggregateCall.getType(), args, 0, false);
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

      public RelTraitSet getTraitSet() {
        return cluster.traitSetOf(Convention.NONE);
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
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> jsonAggs = (List) jsonRel.get(tag);
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
        @SuppressWarnings("unchecked")
        final List<Object> jsonNodes = (List) jsonRel.get(tag);
        final List<RexNode> nodes = new ArrayList<>();
        for (Object jsonNode : jsonNodes) {
          nodes.add(relJson.toRex(this, jsonNode));
        }
        return nodes;
      }

      public List<List<RexNode>> getExpressionListList(String tag) {
        @SuppressWarnings("unchecked")
        final List<List<Object>> jsonNodeListList = (List) jsonRel.get(tag);
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
              @Override public Map.Entry<String, RelDataType> get(int index) {
                return Pair.of(names.get(index),
                    expressionList.get(index).getType());
              }

              @Override public int size() {
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
        throw new UnsupportedOperationException();
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

      public void setLastRel(RelNode relNode) {
        that.setLastRel(relNode);
      }

      public RelNode getLastRel() {
        return that.getLastRel();
      }
    };
    try {
      final RelNode rel = (RelNode) constructor.newInstance(input);
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
    @SuppressWarnings("unchecked")
    final List<Integer> operands = (List<Integer>) jsonAggCall.get("operands");
    final Integer filterOperand = (Integer) jsonAggCall.get("filter");
    final RelDataType type =
        relJson.toType(cluster.getTypeFactory(), jsonAggCall.get("type"));
    return AggregateCall.create(aggregation, distinct, false, operands,
        filterOperand == null ? -1 : filterOperand, type, null);
  }

  private RelNode lookupInput(String jsonInput) {
    RelNode node = relMap.get(jsonInput);
    if (node == null) {
      throw new RuntimeException("unknown id " + jsonInput
          + " for relational expression");
    }
    return node;
  }

  public RelNode getLastRel() {
    return lastRel;
  }

  public void setLastRel(RelNode lastRel) {
    this.lastRel = lastRel;
  }
}

// End RelJsonReader.java
