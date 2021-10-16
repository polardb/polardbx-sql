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

package com.alibaba.polardbx.optimizer.hint.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.common.constants.ExecutorAttribute;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlShuttle;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.sqljep.Comparative;

/**
 * @author chenmo.cm
 */
public abstract class BaseHintOperator implements HintOperator {

    protected final SqlBasicCall hint;
    protected final HintType type;
    protected final Map<HintArgKey, SqlNode> argMap;
    protected final List<Integer> paramIndexMap;
    protected final ExecutionContext ec;

    public BaseHintOperator(SqlBasicCall hint, ExecutionContext ec) {
        this.ec = ec;
        this.hint = hint;
        this.type = HintType.of(hint.getOperator().getName());
        if (type.isA(HintType.ILLEGAL_HINT)) {
            throw new IllegalArgumentException(hint.toString() + " is not a HINT");
        }
        this.paramIndexMap = new ArrayList<>();
        this.argMap = initArg(this.hint, getArgKeys(), this.paramIndexMap);

    }

    @Override
    public HintType getType() {
        return this.type;
    }

    @Override
    public String oldStyle() {
        return "";
    }

    protected abstract List<HintArgKey> getArgKeys();

    private static Map<HintArgKey, SqlNode> initArg(SqlBasicCall hint, List<HintArgKey> argKeys,
                                                    List<Integer> paramIndexMap) {
        final boolean varargs = argKeys.size() <= 0;

        final Map<String, HintArgKey> argKeyMap = new HashMap<>();
        for (HintArgKey argKey : argKeys) {
            argKeyMap.put(argKey.getName(), argKey);
        }

        int argCount = hint.operands.length;
        if (!varargs && argKeys.size() < argCount) {
            argCount = argKeys.size();
        }

        boolean clearParamIndexMap = true;
        final Map<HintArgKey, SqlNode> result = new HashMap<>();
        for (int i = 0; i < argCount; i++) {
            String argKey = null;
            SqlNode argValue = null;

            final SqlNode operand = hint.operands[i];
            if (operand instanceof SqlBasicCall) {
                // equality
                final SqlBasicCall call = (SqlBasicCall) operand;

                if (call.getOperator().getKind() == SqlKind.EQUALS) {
                    final String key = RelUtils.stringValue(call.operands[0]).toUpperCase();

                    if (varargs) {
                        result.put(HintArgKey.of(key, i), call.operands[1]);

                        argKey = key;
                        argValue = call.operands[1];
                    } else if (argKeyMap.containsKey(key)) {
                        result.put(argKeyMap.get(key), call.operands[1]);

                        argKey = key;
                        argValue = call.operands[1];
                    }
                } else {
                    // ignore operator which is not equals
                }
            } else {
                // value only
                HintArgKey hintArgKey = null;
                if (varargs) {
                    hintArgKey = HintArgKey.of("", i);
                } else {
                    hintArgKey = argKeys.get(i);
                }

                result.put(hintArgKey, operand);

                argKey = hintArgKey.getName();
                argValue = operand;
            }

            if (HintArgKey.PARAM_INDEX_MAP_KEY.equalsIgnoreCase(argKey) && null != argValue) {
                if (clearParamIndexMap) {
                    paramIndexMap.clear();
                    clearParamIndexMap = false;
                }

                if (argValue instanceof SqlLiteral) {
                    paramIndexMap.add(RelUtils.integerValue((SqlLiteral) argValue));
                } else if (argValue instanceof SqlBasicCall) {
                    if (argValue.getKind() == SqlKind.ROW) {
                        for (SqlNode sqlNode : ((SqlBasicCall) argValue).operands) {
                            if (sqlNode instanceof SqlLiteral) {
                                paramIndexMap.add(RelUtils.integerValue((SqlLiteral) sqlNode));
                            } else {
                                // ignore unsupported operator kind
                            }
                        } // end of for
                    } else {
                        // only support SqlLiteral or Row
                    }
                } else {
                    // just ignore unsupported format of param index map
                }
            } // end of if
        } // end of for

        return result;
    }

    protected static String stringValue(SqlNode sqlNode) {
        if (sqlNode instanceof SqlCharStringLiteral) {
            return ((SqlCharStringLiteral) sqlNode).getNlsString().getValue();

        }
        return sqlNode.toString();
    }

    protected static Boolean booleanValue(SqlNode sqlNode) {

        if (sqlNode instanceof SqlCharStringLiteral) {
            return Boolean.valueOf(stringValue(sqlNode));
        }

        return (Boolean) ((SqlLiteral) sqlNode).getValue();
    }

    protected static Integer integerValue(SqlLiteral sqlNode) {
        return sqlNode.intValue(false);
    }

    protected static Integer integerValue(SqlNode sqlNode) {
        if (sqlNode instanceof SqlLiteral) {
            return ((SqlLiteral) sqlNode).intValue(false);
        }

        return Integer.valueOf(sqlNode.toString());
    }

    protected static Long longValue(SqlNode sqlNode) {
        if (sqlNode instanceof SqlLiteral) {
            return ((SqlLiteral) sqlNode).longValue(false);
        }

        return Long.valueOf(sqlNode.toString());
    }

    protected static List<String> stringListValue(SqlNode row) {
        List<String> result = new LinkedList<>();

        if (row.getKind() == SqlKind.ROW) {
            for (SqlNode operand : ((SqlBasicCall) row).getOperands()) {
                result.add(stringValue(operand));
            }
        } else if (row.getKind() == SqlKind.LITERAL) {
            result.add(stringValue(row));
        }

        return result;
    }

    protected static SqlNode updateParamIndex(SqlNode node, final List<Integer> paramIndex) {
        return node.accept(new SqlShuttle() {

            @Override
            public SqlNode visit(SqlDynamicParam param) {
                if (param.getIndex() < paramIndex.size()) {
                    return new SqlDynamicParam(paramIndex.get(param.getIndex()),
                        param.getParserPosition(),
                        param.getValue());
                } else {
                    return super.visit(param);
                }
            }
        });
    }

    protected MySqlSelectQueryBlock parseQuery(String sql) {
        SQLSelectStatement sqlSelect =
            (SQLSelectStatement) FastsqlUtils.parseSql(sql, SQLParserFeature.IgnoreNameQuotes).get(0);
        return (MySqlSelectQueryBlock) sqlSelect.getSelect().getQuery();
    }

    protected List<String> convertGroupIndex(String schemaName, List<String> groups) {
        final List<Group> allGroups = OptimizerContext.getContext(schemaName).getMatrix().getGroups();

        List<String> result = new LinkedList<>();
        for (String group : groups) {
            String groupKey = convertGroupNumber(allGroups, group);

            result.add(groupKey);
        }
        return result;
    }

    protected List<String> convertGroupIndex(String schemaName, List<String> groups, HintCmdOperator.CmdBean current) {
        final List<Group> allGroups = OptimizerContext.getContext(schemaName).getMatrix().getGroups();

        List<String> result = new LinkedList<>();
        for (String group : groups) {
            String groupKey = convertGroupNumber(allGroups, group);

            if (ExecutorAttribute.DEFAULT_DB.equalsIgnoreCase(groupKey)) {
                groupKey = OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(null);
            } else if (ExecutorAttribute.SHADOW_DB.equalsIgnoreCase(groupKey)) {
                groupKey = OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(null);
                current.getExtraCmd().put(ExecutorAttribute.QUERY_SHADOW_DB, Boolean.TRUE);
            } else if (ExecutorAttribute.INFORMATION_SCHEMA.equalsIgnoreCase(group)) {
                groupKey = OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(null);
                current.getExtraCmd().put(ExecutorAttribute.QUERY_INFORMATION_SCHEMA, Boolean.TRUE);
            } else if (ExecutorAttribute.PERFORMANCE_SCHEMA.equalsIgnoreCase(group)) {
                groupKey = OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(null);
                current.getExtraCmd().put(ExecutorAttribute.QUERY_PERFORMANCE_SCHEMA, Boolean.TRUE);
            }

            result.add(groupKey);
        }
        return result;
    }

    private String convertGroupNumber(List<Group> allGroups, String group) {
        String groupKey = group;
        try {
            Integer groupIndex = Integer.valueOf(group);
            if (groupIndex < allGroups.size()) {
                groupKey = allGroups.get(groupIndex).getName();
            }
        } catch (Exception e) {
            // using total name of group
        }
        return groupKey;
    }

    public static Map<String, Map<String, Comparative>> buildComparative(String table, String condition,
                                                                         List<Integer> paramIndexMap,
                                                                         String schemaName, ExecutionContext ec) {
        final String sql = HintUtil.buildPushdown(table, condition, schemaName);

        final SqlNodeList astList = new FastsqlParser().parse(sql);

        // validate
        SqlConverter converter = SqlConverter.getInstance(schemaName, ec);
        SqlNode validatedNode = converter.validate(updateParamIndex(astList.get(0), paramIndexMap));

        // logical plan
        RelNode rel = converter.toRel(validatedNode);

        Map<String, Map<String, Comparative>> comparative = new HashMap<>();
        ConditionExtractor.partitioningConditionFrom(rel).extract().allCondition(comparative, null, ec);

        return comparative;
    }

    public static Map<String, PartitionPruneStep> buildPartitionPruneStepMap(String table, String condition,
                                                                   List<Integer> paramIndexMap,
                                                                   String schemaName, ExecutionContext ec) {
        final String sql = HintUtil.buildPushdown(table, condition, schemaName);

        final SqlNodeList astList = new FastsqlParser().parse(sql);

        // validate
        SqlConverter converter = SqlConverter.getInstance(schemaName, ec);
        SqlNode validatedNode = converter.validate(updateParamIndex(astList.get(0), paramIndexMap));

        // logical plan
        RelNode rel = converter.toRel(validatedNode);

        Map<String, PartitionPruneStep> result =
            ConditionExtractor.partitioningConditionFrom(rel).extract().allPartPruneSteps( ec);

        return result;
    }
}
