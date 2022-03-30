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

package com.alibaba.polardbx.optimizer.hint.util;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdQueryBlockName;
import com.alibaba.polardbx.optimizer.hint.operator.HintPushdownOperator;
import com.alibaba.polardbx.optimizer.parse.HintParser;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.sharding.DataNodeChooser;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.convertTargetDB;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.filterGroup;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.getGroupIntersection;

/**
 * @author chenmo.cm
 */
public class HintUtil {

    public static boolean enableForbidPushDmlWithHint = true;

    public static String buildAggSql(String agg, String groupBy, String having) {
        StringBuilder sql = new StringBuilder("SELECT ");
        if (TStringUtil.isNotBlank(agg)) {
            sql.append(agg);
        } else {
            sql.append("*");
        }

        sql.append(" FROM DUAL ");

        if (TStringUtil.isNotBlank(groupBy)) {
            sql.append(" GROUP BY ").append(groupBy);
        }

        if (TStringUtil.isNotBlank(having)) {
            sql.append(" HAVING ").append(having);
        }
        return sql.toString();
    }

    public static String buildProjectSql(String projcet) {
        StringBuilder sql = new StringBuilder("SELECT ");
        if (TStringUtil.isNotBlank(projcet)) {
            sql.append(projcet);
        } else {
            sql.append("*");
        }

        sql.append(" FROM DUAL ");

        return sql.toString();
    }

    public static String buildFilterSql(String filter) {
        StringBuilder sql = new StringBuilder("SELECT * FROM DUAL");
        if (TStringUtil.isNotBlank(filter)) {
            sql.append(" WHERE ").append(filter);
        }

        return sql.toString();
    }

    public static String buildLimitSql(String limit) {
        StringBuilder sql = new StringBuilder("SELECT * FROM DUAL");
        if (TStringUtil.isNotBlank(limit)) {
            sql.append(" LIMIT ").append(limit);
        }

        return sql.toString();
    }

    public static String buildSortSql(String sort) {
        StringBuilder sql = new StringBuilder("SELECT * FROM DUAL");
        if (TStringUtil.isNotBlank(sort)) {
            sql.append(" ORDER BY ").append(sort);
        }

        return sql.toString();
    }

    public static String buildFromSql(String from) {
        StringBuilder sql = new StringBuilder("SELECT * ");
        if (TStringUtil.isNotBlank(from)) {
            sql.append(" FROM ").append(from);
        } else {
            sql.append(" FROM DUAL");
        }

        return sql.toString();
    }

    public static String buildPushdown(String table, String condition, String schemaName) {
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        if (TStringUtil.isNotBlank(schemaName)) {
            sql.append("`").append(schemaName).append("`").append(".");
        }

        if (TStringUtil.isNotBlank(table)) {
            sql.append(table);
        } else {
            sql.append("DUAL");
        }

        if (TStringUtil.isNotBlank(condition)) {
            sql.append(" WHERE ").append(condition);
        }

        return sql.toString();
    }

    public static ByteString convertSimpleHint(ByteString sql, Map<String, Object> extraCmds) {
        if (HintParser.containHint(sql)) {
            String hint = HintParser.getInstance().getTddlSimpleHint(sql);

            if (!TStringUtil.isEmpty(hint)) {
                if (hint != null && !hint.trim().startsWith("plan")) {
                    hint = replaceSquareBrackets(hint);
                }
                String newHint = HintParser.TDDL_NEW_HINT_PREFIX + hint + HintParser.TDDL_NEW_HINT_END;
                sql = HintParser.getInstance().exchangeSimpleHint(hint, newHint, sql);
            }
            return sql;
        } else {
            return sql;
        }
    }

    /**
     * 把方括号 <pre>[...|...]</pre> 替换成 <pre>"...;..."</pre>
     */
    public static String replaceSquareBrackets(String hint) {
        int left, right;
        if ((left = hint.indexOf('[')) != -1 && left < (right = hint.indexOf(']'))) {
            StringBuilder result = new StringBuilder();
            result.append(hint.substring(0, left)).append('"');
            result.append(hint.substring(left + 1, right).replace('|', ';'));
            result.append('"').append(hint.substring(right + 1));
            return result.toString();
        }
        return hint;
    }

    public static List<String> splitAndTrim(String str, String separator) {
        String[] splited = TStringUtil.split(str, separator);

        if (null == splited || splited.length == 0) {
            return ImmutableList.of();
        }

        final List<String> result = new ArrayList<>(splited.length);

        for (String aSplit : splited) {
            result.add(TStringUtil.trim(aSplit));
        }

        return result;
    }

    public static boolean emptyCollection(Collection c) {
        return null == c || c.isEmpty();
    }

    public static HintConverter.HintCollection collectHint(
        SqlNodeList hints, HintConverter.HintCollection collection, boolean testMode, ExecutionContext ec) {
        if (null != hints) {
            for (SqlNode hint : hints) {
                for (SqlNode groupNode : (SqlNodeList) hint) {
                    collection
                        .merge(HintConverter.convertCmd((SqlNodeList) groupNode, new LinkedList<>(), testMode, ec));
                }
            }
        }

        return collection;
    }

    public static String gerenateQbName() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    public static boolean sameQueryBlock(HintPushdownOperator pushdownHint, HintCmdQueryBlockName qbNameHint) {
        if (null == pushdownHint || null == qbNameHint) {
            return false;
        }

        return TStringUtil.equals(pushdownHint.qbName, qbNameHint.qbName);
    }

    public static List<String> mergeTableNames(List<String> logicalTable, List<String> virtualTable,
                                               List<String> realTable) {
        List<String> phyTables = new LinkedList<>();
        // merge vtName and tableNames
        for (int lIndex = 0, pIndex = 0; lIndex < logicalTable.size(); lIndex++) {
            if (pIndex < virtualTable.size() && pIndex < realTable.size()
                && TStringUtil.equals(logicalTable.get(lIndex), virtualTable.get(pIndex))) {
                phyTables.add(realTable.get(pIndex));
                pIndex++;
            } else {
                phyTables.add(TStringUtil.trim(logicalTable.get(lIndex)));
            } // end of else
        } // end of for
        return phyTables;
    }

    public static Map<String, List<List<String>>> buildTargetTables(List<String> tables,
                                                                    Map<String, Map<String, Comparative>> comparative,
                                                                    Map<Integer, ParameterContext> param,
                                                                    String schemaName, ExecutionContext ec) {
        List<List<TargetDB>> targetDBs = DataNodeChooser.shard(ImmutableList.copyOf(tables),
            param,
            comparative,
            schemaName, ec);
        final Set<String> groupIntersection = getGroupIntersection(targetDBs);
        targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
        return convertTargetDB(targetDBs);
    }

    public static Map<String, List<List<String>>> buildTargetTablesByPruneStepMap(List<String> tables,
                                                                    Map<String, PartitionPruneStep> pruneStepMap,
                                                                    String schemaName, ExecutionContext ec) {
        List<List<TargetDB>> targetDBs = DataNodeChooser.shardByPruneStep(ImmutableList.copyOf(tables),
            pruneStepMap,
            schemaName, ec);
        final Set<String> groupIntersection = getGroupIntersection(targetDBs);
        targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
        return convertTargetDB(targetDBs);
    }

    public static List<String> allGroup() {
        return allGroup(null);
    }

    public static List<String> allGroup(String schemaName) {
        final List<Group> allGroups = OptimizerContext.getContext(schemaName).getMatrix().getGroups();
        List<String> result = new LinkedList<>();
        for (Group group : allGroups) {
            if (GroupType.MYSQL_JDBC == group.getType()) {
                result.add(group.getName());
            }
        }
        return result;
    }

    /**
     * Get all groups contains broadcast table, except scale-in and scale-out groups
     */
    public static List<String> allGroupsWithBroadcastTable(String schema) {
        final List<Group> allGroups = OptimizerContext.getContext(schema).getMatrix().getGroups();
        final DbGroupInfoManager dbGroupInfoManager = DbGroupInfoManager.getInstance();

        List<String> result = new LinkedList<>();
        for (Group group : allGroups) {
            DbGroupInfoRecord info = dbGroupInfoManager.queryGroupInfo(schema, group.getName());

            if (GroupType.MYSQL_JDBC == group.getType() && info != null && info.isNormal()) {
                result.add(group.getName());
            }
        }

        return result;
    }

    public static boolean forbidPushDmlWithHint() {
        if (enableForbidPushDmlWithHint) {
            return true;
        } else {
            return false;
        }
    }

    public static String findTestTableName(String name, boolean testMode, Map<String, String> tables) {
        if (testMode && tables != null) {
            String relName = tables.get(name);
            if (relName != null) {
                return relName;
            }
        }
        return name;
    }
}
