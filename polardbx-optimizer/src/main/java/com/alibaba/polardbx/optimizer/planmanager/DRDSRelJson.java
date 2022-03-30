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

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeAND;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.common.model.sqljep.ExtComparative;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.PushDownOpt;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.GroupConcatAggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUserVar;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlRuntimeFilterBuildFunction;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Utilities for converting {@link RelNode}
 * into JSON format.
 */
public class DRDSRelJson extends RelJson {

    private boolean supportMpp;

    public DRDSRelJson(JsonBuilder jsonBuilder, boolean supportMpp) {
        super(jsonBuilder);
        this.supportMpp = supportMpp;
    }

    public RelDistribution toDistribution(Map<String, Object> map) {
        List<Integer> keys = (List<Integer>) map.get("keys");
        RelDistribution.Type type = RelDistribution.Type.valueOf(map.get("type").toString());
        return new RelDistributions.RelDistributionImpl(type, ImmutableIntList.copyOf(keys));
    }

    @Override
    protected Object toJson(Object value) {
        if (value == null
            || value instanceof Number
            || value instanceof String
            || value instanceof Boolean) {
            return value;
        } else if (value instanceof Window.RexWinAggCall) {
            return toJson((Window.RexWinAggCall) value);
        } else if (value instanceof RexNode) {
            return toJson((RexNode) value);
        } else if (value instanceof CorrelationId) {
            return toJson((CorrelationId) value);
        } else if (value instanceof List) {
            final List<Object> list = jsonBuilder.list();
            for (Object o : (List) value) {
                list.add(toJson(o));
            }
            return list;
        } else if (value instanceof Map) {
            final Map<String, Object> map = jsonBuilder.map();
            for (Object key : ((Map) value).keySet()) {
                map.put(toJson(key).toString(), toJson(((Map) value).get(key)));
            }
            return map;
        } else if (value instanceof ImmutableBitSet) {
            final List<Object> list = jsonBuilder.list();
            for (Integer integer : (ImmutableBitSet) value) {
                list.add(toJson(integer));
            }
            return list;
        } else if (value instanceof Set) {
            final List<Object> list = jsonBuilder.list();
            for (Object object : (Set) value) {
                list.add(toJson(object));
            }
            return list;
        } else if (value instanceof AggregateCall) {
            return toJson((AggregateCall) value);
        } else if (value instanceof RelCollationImpl) {
            return toJson((RelCollationImpl) value);
        } else if (value instanceof RelDataType) {
            return toJson((RelDataType) value);
        } else if (value instanceof RelDataTypeField) {
            return toJson((RelDataTypeField) value);
        } else if (value instanceof RelOptTableImpl) {
            return toJson((RelOptTableImpl) value);
        } else if (value instanceof CalciteCatalogReader) {
            return toJson((CalciteCatalogReader) value);
        } else if (value instanceof CalciteConnectionConfigImpl) {
            return toJson((CalciteConnectionConfigImpl) value);
        } else if (value instanceof PushDownOpt) {
            return toJson((PushDownOpt) value);
        } else if (value instanceof RelNode) {
            return toJson((RelNode) value);
        } else if (value instanceof Pair) {
            return toJson((Pair) value);
        } else if (value instanceof Map) {
            return toJson((Map) value);
        } else if (value instanceof Comparative) {
            return toJson((Comparative) value);
        } else if (value instanceof RelDistribution) {
            return toJson((RelDistribution) value);
        } else if (value instanceof SqlOperator) {
            return toJson((SqlOperator) value);
        } else if (value instanceof Window.Group) {
            return toJson((Window.Group) value);
        } else if (value instanceof RexWindowBound) {
            return toJson((RexWindowBound) value);
        } else if (value instanceof SqlKind) {
            return toJson((SqlKind) value);
        } else {
            throw new UnsupportedOperationException("type not serializable: "
                + value + " (type " + value.getClass().getCanonicalName() + ")");
        }
    }

    protected Object toJson(SqlKind value) {
        return value.name();
    }

    private Object toJson(Window.Group node) {
        final Map<String, Object> map = jsonBuilder.map();
        map.put("keys", toJson(node.keys.asList()));
        map.put("isRows", node.isRows);
        map.put("lowerBound", toJson(node.lowerBound));
        map.put("upperBound", toJson(node.upperBound));
        map.put("calls", toJson(node.aggCalls));
        map.put("orderKeys", toJson(node.orderKeys));
        return map;
    }

    private Object toJson(RexWindowBound node) {
        final Map<String, Object> map = jsonBuilder.map();
        map.put("unbounded", node.isUnbounded());
        map.put("currentRow", node.isCurrentRow());
        map.put("offset", toJson(node.getOffset()));
        map.put("orderKey", toJson(node.getOrderKey()));
        map.put("following", toJson(node.isFollowing()));
        map.put("preceding", toJson(node.isPreceding()));
        return map;
    }

    private Object toJson(RelDistribution node) {
        final Map<String, Object> map = jsonBuilder.map();
        map.put("type", toJson(node.getType().name()));
        map.put("keys", toJson(node.getKeys()));
        return map;
    }

    private Object toJson(Comparative comparative) {
        if (comparative instanceof ComparativeAND) {
            final Map<String, Object> map = jsonBuilder.map();
            map.put("type", ComparativeAND.class.getSimpleName());
            map.put("list", toJson(((ComparativeAND) comparative).getList()));
            return map;
        } else if (comparative instanceof ComparativeOR) {
            final Map<String, Object> map = jsonBuilder.map();
            map.put("type", ComparativeOR.class.getSimpleName());
            map.put("list", toJson(((ComparativeOR) comparative).getList()));
            return map;
        } else if (comparative instanceof ExtComparative) {
            final Map<String, Object> map = jsonBuilder.map();
            map.put("type", ExtComparative.class.getSimpleName());
            map.put("comparison", comparative.getComparison());
            map.put("value", toJson(comparative.getValue()));
            map.put("columnName", ((ExtComparative) comparative).getColumnName());
            return map;
        } else {
            final Map<String, Object> map = jsonBuilder.map();
            map.put("type", Comparative.class.getSimpleName());
            map.put("comparison", comparative.getComparison());
            map.put("value", toJson(comparative.getValue()));
            return map;
        }
    }

    private Object toJson(Map node) {
        jsonBuilder.map();
        return node;
    }

    private Object toJson(Pair node) {
        final Map<String, Object> map = jsonBuilder.map();
        map.put("left", toJson(node.getKey()));
        map.put("right", toJson(node.getValue()));
        return map;
    }

    private Map<String, Object> toJson(RelNode node, String key) {
        final DRDSRelJsonWriter writer = new DRDSRelJsonWriter(this.supportMpp);
        node.explain(writer);
        return writer.asMap(key);
    }

    private Object toJson(PushDownOpt node) {
        final Map<String, Object> map = jsonBuilder.map();
        map.putAll(toJson(node.getPushedRelNode(), "pushrels"));
        return map;
    }

    private Object toJson(CalciteConnectionConfigImpl node) {
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(node.caseSensitive()));
        final Map<String, Object> map = jsonBuilder.map();
        map.put("properties", properties);
        return map;
    }

    private Object toJson(CalciteCatalogReader node) {
        final Map<String, Object> map = jsonBuilder.map();
        map.put("rootSchema", null);
        map.put("typeFactory", node.getTypeFactory());
        map.put("schemaPaths", node.getSchemaPaths());
        map.put("nameMatcher", SqlNameMatchers.withCaseSensitive(node.nameMatcher().isCaseSensitive()));
        map.put("config", toJson(node.getConfig()));
        return map;
    }

    private Object toJson(RelOptTableImpl node) {
        final Map<String, Object> map = jsonBuilder.map();

        map.put("type", "RelOptTableImpl");
        map.put("schema", null);
        map.put("rowType", toJson(node.getRowType()));
        map.put("table", null);

        map.put("names", node.getQualifiedName());
        return map;
    }

    public RelNode fromJson(List<Map<String, Object>> json, RelInput relInput) {
        DRDSRelJsonReader drdsRelJsonReader = new DRDSRelJsonReader(relInput.getCluster(),
            SqlConverter.getInstance(PlannerContext.getPlannerContext(relInput.getCluster()).getExecutionContext())
                .getCatalog(),
            null, this.supportMpp);
        try {
            RelNode rel = drdsRelJsonReader.read(json);
            return rel;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("PLAN EXTERNALIZE TEST error:" + e.getMessage());
        }
    }

    @Override
    protected Object toJson(RexNode node) {
        final Map<String, Object> map;
        if (node == null) {
            return null;
        }
        switch (node.getKind()) {
        case FIELD_ACCESS:
            map = jsonBuilder.map();
            final RexFieldAccess fieldAccess = (RexFieldAccess) node;
            map.put("field", fieldAccess.getField().getName());
            map.put("expr", toJson(fieldAccess.getReferenceExpr()));
            return map;
        case LITERAL:
            final RexLiteral literal = (RexLiteral) node;
            final Object value2 = literal.getValue2();
            if (value2 == null) {
                // Special treatment for null literal because (1) we wouldn't want
                // 'null' to be confused as an empty expression and (2) for null
                // literals we need an explicit type.
                map = jsonBuilder.map();
                map.put("literal", null);
                map.put("type", literal.getTypeName().name());
                return map;
            } else if (value2 instanceof TimeUnitRange) {
                TimeUnitRange timeUnitRange = (TimeUnitRange) value2;
                map = jsonBuilder.map();
                map.put("literal", "TimeUnitRange");
                map.put("type", literal.getTypeName().name());
                map.put("TimeUnitRangeType", timeUnitRange.toString());
                return map;
            } else if (value2 instanceof TimeUnit) {
                TimeUnit timeUnit = (TimeUnit) value2;
                map = jsonBuilder.map();
                map.put("literal", "TimeUnit");
                map.put("type", literal.getTypeName().name());
                map.put("ordinal", timeUnit.ordinal());
                return map;
            } else if (value2 instanceof SqlTypeName) {
                SqlTypeName typeName = (SqlTypeName) value2;
                map = jsonBuilder.map();
                map.put("literal", "SqlTypeName");
                map.put("type", literal.getTypeName().name());
                map.put("ordinal", typeName.ordinal());
                return map;
            } else if (value2 instanceof SqlTrimFunction.Flag) {
                map = jsonBuilder.map();
                map.put("literal", "SqlTrimFunctionflag");
                map.put("type", literal.getTypeName().name());
                map.put("flag", ((SqlTrimFunction.Flag) value2).toString());
                return map;
            } else if (value2 instanceof ByteString) {
                map = jsonBuilder.map();
                map.put("literal", "ByteString");
                map.put("type", literal.getTypeName().name());
                map.put("value", ((ByteString) value2).toBase64String());
                return map;
            }
            return value2;
        case INPUT_REF:
        case LOCAL_REF:
            map = jsonBuilder.map();
            map.put("input", ((RexSlot) node).getIndex());
            map.put("name", ((RexSlot) node).getName());
            return map;
        case CORREL_VARIABLE:
            map = jsonBuilder.map();
            map.put("correl", ((RexCorrelVariable) node).getName());
            map.put("type", toJson(node.getType()));
            return map;
        case DYNAMIC_PARAM:
            map = jsonBuilder.map();
            map.put("index", ((RexDynamicParam) node).getIndex());
            map.put("reltype", toJson(node.getType()));
            map.put("type", "DYNAMIC");
            if (((RexDynamicParam) node).getSemiType() != null) {
                map.put("semitype", ((RexDynamicParam) node).getSemiType().name());
            }
            RexDynamicParam rexDynamicParam = (RexDynamicParam) node;
            if (((RexDynamicParam) node).getRel() != null) {
                map.putAll(toJson(rexDynamicParam.getRel(), "rel"));
            }
            if (rexDynamicParam.getSubqueryKind() != null) {
                map.put("subqueryKind", rexDynamicParam.getSubqueryKind().name());
            }
            if (rexDynamicParam.getSubqueryOperands() != null) {
                map.put("subqueryOperands", toJson(rexDynamicParam.getSubqueryOperands()));
            }
            if (rexDynamicParam.getSubqueryOp() != null) {
                map.put("subqueryOp", toJson(rexDynamicParam.getSubqueryOp()));
            }
            if (!rexDynamicParam.isMaxOnerow()) {
                map.put("maxonerow", "false");
            }
            return map;
        case RUNTIME_FILTER_BUILD:
            map = jsonBuilder.map();
            SqlRuntimeFilterBuildFunction buildFunction =
                (SqlRuntimeFilterBuildFunction) ((RexCall) node).getOperator();
            map.put("runtimeFilterIds", toJson(buildFunction.getRuntimeFilterIds()));
            map.put("ndv", buildFunction.getNdv());
            final List<Object> listOps = jsonBuilder.list();
            for (RexNode operand : ((RexCall) node).getOperands()) {
                listOps.add(toJson(operand));
            }
            map.put("operands", listOps);
            return map;
        case RUNTIME_FILTER:
            map = jsonBuilder.map();
            SqlRuntimeFilterFunction filterFunction = (SqlRuntimeFilterFunction) ((RexCall) node).getOperator();
            map.put("runtimeFilterId", filterFunction.getId());
            map.put("guessSelectivity", filterFunction.getGuessSelectivity());
            map.put("usingXxHash", filterFunction.isUsingXxHash());
            final List<Object> listFilter = jsonBuilder.list();
            for (RexNode operand : ((RexCall) node).getOperands()) {
                listFilter.add(toJson(operand));
            }
            map.put("operands", listFilter);
            return map;
        case USER_VAR:
            RexUserVar rexUserVar = (RexUserVar) node;
            map = jsonBuilder.map();
            map.put("UserVar", null);
            map.put("name", rexUserVar.getName());
            return map;
        default:
            if (node instanceof RexCall) {
                final RexCall call = (RexCall) node;
                map = jsonBuilder.map();
                map.put("op", toJson(call.getOperator()));
                final List<Object> list = jsonBuilder.list();
                for (RexNode operand : call.getOperands()) {
                    list.add(toJson(operand));
                }
                map.put("operands", list);
                map.put("type", toJson(node.getType()));

                if (call.getOperator() instanceof SqlFunction) {
                    if (((SqlFunction) call.getOperator()).getFunctionType().isUserDefined()) {
                        map.put("class", call.getOperator().getClass().getName());
                    }
                }

                if (call instanceof RexSubQuery) {
                    map.putAll(toJson(((RexSubQuery) call).rel, "rels"));
                    map.put("subquery", "1");
                }
                return map;
            }
            throw new UnsupportedOperationException("unknown rex " + node);
        }
    }

    @Override
    public RexNode toRex(RelInput relInput, Object o) {
        final RelOptCluster cluster = relInput.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        if (o == null) {
            return null;
        } else if (o instanceof Map) {
            Map map = (Map) o;
            final String op = (String) map.get("op");
            final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
            if (op != null) {
                final List operands = (List) map.get("operands");
                final Object jsonType = map.get("type");

                final SqlOperator operator = toOp(op, map);
                final List<RexNode> rexOperands = toRexList(relInput, operands);
                RelDataType type;
                if (jsonType != null) {
                    type = toType(typeFactory, jsonType);
                } else {
                    type = rexBuilder.deriveReturnType(operator, rexOperands);
                }

                if (map.get("subquery") != null && map.get("subquery").equals("1")) {
                    return new RexSubQuery(type,
                        operator,
                        ImmutableList.copyOf(rexOperands),
                        fromJson((List<Map<String, Object>>) map.get("rels"), relInput));
                }

                return rexBuilder.makeCall(type, operator, rexOperands);
            }
            final Integer input = (Integer) map.get("input");
            if (input != null) {
                List<RelNode> inputNodes = relInput.getInputs();
                int i = input;
                for (RelNode inputNode : inputNodes) {
                    final RelDataType rowType = inputNode.getRowType();
                    if (i < rowType.getFieldCount()) {
                        final RelDataTypeField field = rowType.getFieldList().get(i);
                        return rexBuilder.makeInputRef(field.getType(), input);
                    }
                    i -= rowType.getFieldCount();
                }
                if (relInput.get("constants") != null && relInput.getIntegerList("constants").size() > 0) {
                    return rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), input);
                }
                throw new RuntimeException("input field " + input + " is out of range");
            }
            final String field = (String) map.get("field");
            if (field != null) {
                final Object jsonExpr = map.get("expr");
                final RexNode expr = toRex(relInput, jsonExpr);
                return rexBuilder.makeFieldAccess(expr, field, true);
            }
            final String correl = (String) map.get("correl");
            if (correl != null) {
                final Object jsonType = map.get("type");
                RelDataType type = toType(typeFactory, jsonType);
                return rexBuilder.makeCorrel(type, new CorrelationId(correl));
            }
            if (map.containsKey("UserVar")) {
                return rexBuilder.makeUserSymbolLiteral(map.get("name").toString());
            }
            if (map.containsKey("literal")) {
                final Object literal = map.get("literal");
                final SqlTypeName sqlTypeName = Util.enumVal(SqlTypeName.class, (String) map.get("type"));
                if (literal == null) {
                    return rexBuilder.makeNullLiteral(typeFactory.createSqlType(sqlTypeName));
                } else if (literal.equals("TimeUnitRange")) {
                    return rexBuilder
                        .makeLiteral(Util.enumVal(TimeUnitRange.class, (String) map.get("TimeUnitRangeType")),
                            typeFactory.createSqlType(sqlTypeName),
                            sqlTypeName);
                } else if (literal.equals("TimeUnit")) {
                    return rexBuilder.makeLiteral(TimeUnit.getValue((int) map.get("ordinal")),
                        typeFactory.createSqlType(sqlTypeName),
                        sqlTypeName);
                } else if (literal.equals("SqlTypeName")) {
                    return rexBuilder.makeLiteral(SqlTypeName.getValue((int) map.get("ordinal")),
                        typeFactory.createSqlType(sqlTypeName),
                        sqlTypeName);
                } else if (literal.equals("SqlTrimFunctionflag")) {
                    return rexBuilder.makeLiteral(SqlTrimFunction.Flag.valueOf((String) map.get("flag")),
                        typeFactory.createSqlType(sqlTypeName),
                        sqlTypeName);
                } else if (literal.equals("ByteString")) {
                    String value = (String) map.get("value");
                    return rexBuilder.makeBinaryLiteral(ByteString.ofBase64(value));
                }
                return toRex(relInput, literal);
            }
            final String type = (String) map.get("type");
            if (type != null && type.equals("DYNAMIC")) {
                if (map.get("rel") != null) {
                    SqlKind sqlKind = null;
                    SqlOperator sqlOperator = null;
                    SemiJoinType semiJoinType = null;
                    boolean maxonerow = true;
                    ImmutableList<RexNode> subqueryOperands = ImmutableList.of();
                    if (map.get("subqueryKind") != null) {
                        sqlKind = Enum.valueOf(SqlKind.class, (String) map.get("subqueryKind"));
                    }
                    if (map.get("subqueryOperands") != null) {
                        final List<RexNode> nodes = new ArrayList<>();
                        for (Object jsonNode : (List) map.get("subqueryOperands")) {
                            nodes.add(toRex(relInput, jsonNode));
                        }
                        subqueryOperands = ImmutableList.copyOf(nodes);
                    }
                    if (map.get("subqueryOp") != null) {
                        sqlOperator = toOp((String) map.get("subqueryOp"), new HashMap<>());
                    }
                    if (map.get("semitype") != null) {
                        semiJoinType = Enum.valueOf(SemiJoinType.class, (String) map.get("semitype"));
                    }
                    if (map.get("maxonerow") != null && "false".equalsIgnoreCase((String) map.get("maxonerow"))) {
                        maxonerow = false;
                    }
                    RexDynamicParam rexDynamicParam = new RexDynamicParam(toType(typeFactory, map.get("reltype")),
                        (Integer) map.get("index"),
                        fromJson((List<Map<String, Object>>) map.get("rel"), relInput), subqueryOperands, sqlOperator,
                        sqlKind);
                    rexDynamicParam.setSemiType(semiJoinType);
                    rexDynamicParam.setMaxOnerow(maxonerow);
                    return rexDynamicParam;
                }
                RexDynamicParam rexDynamicParam = new RexDynamicParam(toType(typeFactory, map.get("reltype")),
                    (Integer) map.get("index"));
                return rexDynamicParam;
            }

            if (map.containsKey("runtimeFilterIds")) {
                final List<Integer> runtimeFilterIds = (List<Integer>) map.get("runtimeFilterIds");
                double ndv = ((BigDecimal) map.get("ndv")).doubleValue();
                final List<RexNode> operands = new ArrayList<>();
                for (Object jsonNode : (List) map.get("operands")) {
                    operands.add(toRex(relInput, jsonNode));
                }
                SqlRuntimeFilterBuildFunction buildFunction =
                    new SqlRuntimeFilterBuildFunction(runtimeFilterIds, ndv);
                return rexBuilder.makeCall(buildFunction, operands);
            }

            if (map.containsKey("runtimeFilterId")) {
                final Integer runtimeFilterId = (Integer) map.get("runtimeFilterId");
                double guessSelectivity = ((BigDecimal) map.get("guessSelectivity")).doubleValue();
                boolean usingXxHash = ((Boolean) map.get("usingXxHash"));
                final List<RexNode> operands = new ArrayList<>();
                for (Object jsonNode : (List) map.get("operands")) {
                    operands.add(toRex(relInput, jsonNode));
                }
                SqlRuntimeFilterFunction filterFunction =
                    new SqlRuntimeFilterFunction(runtimeFilterId, guessSelectivity, usingXxHash);
                return rexBuilder.makeCall(filterFunction, operands);
            }

            throw new UnsupportedOperationException("cannot convert to rex " + o);
        } else if (o instanceof Boolean) {
            return rexBuilder.makeLiteral((Boolean) o);
        } else if (o instanceof String) {
            return rexBuilder.makeLiteral((String) o);
        } else if (o instanceof Number) {
            final Number number = (Number) o;
            if (number instanceof Double || number instanceof Float) {
                return rexBuilder.makeApproxLiteral(
                    BigDecimal.valueOf(number.doubleValue()));
            } else {
                return rexBuilder.makeBigIntLiteral(number.longValue());
            }
        } else {
            throw new UnsupportedOperationException("cannot convert to rex " + o);
        }
    }

    protected SqlOperator toOp(String op, Map<String, Object> map) {
        // TODO: build a map, for more efficient lookup
        // TODO: look up based on SqlKind
        final List<SqlOperator> operatorList =
            TddlOperatorTable.instance().getOperatorList();
        for (SqlOperator operator : operatorList) {
            if ((operator.getClass().getSimpleName() + operator.getName()).equals(op)) {
                return operator;
            }
        }
        String class_ = (String) map.get("class");
        if (class_ != null) {
            return AvaticaUtils.instantiatePlugin(SqlOperator.class, class_);
        }
        return null;
    }

    public Object toJson(AggregateCall node) {
        final Map<String, Object> map = jsonBuilder.map();
        map.put("agg", toJson(node.getAggregation()));
        map.put("type", toJson(node.getType()));
        map.put("distinct", node.isDistinct());
        map.put("operands", node.getArgList());
        map.put("filter", node.filterArg);
        if (node instanceof GroupConcatAggregateCall) {
            GroupConcatAggregateCall groupConcatAggregateCall = (GroupConcatAggregateCall) node;
            map.put("isGroupConcat", toJson(true));
            map.put("orderList", toJson(groupConcatAggregateCall.getOrderList()));
            map.put("ascOrDescList", toJson(groupConcatAggregateCall.getAscOrDescList()));
            map.put("separator", groupConcatAggregateCall.getSeparator());
        }
        return map;
    }

    public Object toJson(Window.RexWinAggCall node) {
        final Map<String, Object> map = jsonBuilder.map();
        map.put("agg", toJson(node.getOperator()));
        map.put("type", toJson(node.getType()));
        map.put("distinct", false);
        map.put("operands", toJson(node.getOperands()));
        map.put("ordinal", node.ordinal);
        return map;
    }
}

// End RelJson.java
