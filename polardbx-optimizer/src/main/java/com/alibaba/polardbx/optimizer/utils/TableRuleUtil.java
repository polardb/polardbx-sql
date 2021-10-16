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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionBy;
import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionDefinition;
import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionOptions;
import com.alibaba.polardbx.optimizer.parse.bean.TBPartitionBy;
import com.alibaba.polardbx.optimizer.parse.bean.TBPartitionDefinition;
import com.alibaba.polardbx.optimizer.utils.newrule.IPartitionGen;
import com.alibaba.polardbx.optimizer.utils.newrule.ISubpartitionGen;
import com.alibaba.polardbx.optimizer.utils.newrule.RuleUtils;
import com.alibaba.polardbx.optimizer.utils.newrule.ShardFuncParamsChecker;
import com.alibaba.polardbx.optimizer.utils.newrule.TableRuleGenFactory;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.rule.ddl.PartitionByType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableRuleUtil {

    public static TableRule buildShardingTableRule(String tableName, TableMeta tableToSchema, SqlNode dbPartitionBy,
                                                   SqlNode dbPartitions, SqlNode tbPartitionBy, SqlNode tbPartitions,
                                                   OptimizerContext optimizerContext,
                                                   ExecutionContext executionContext) {
        DBPartitionBy dbPartitionByRule = new DBPartitionBy();
        TBPartitionBy tbPartitionByRule = new TBPartitionBy();
        DBPartitionOptions dbPartitionOptions = new DBPartitionOptions();

        if (dbPartitionBy != null) {
            final List<String> paramNames = new ArrayList<>();
            final SqlBasicCall dbFunBasicCall = (SqlBasicCall) dbPartitionBy;
            final SqlOperator operator = dbFunBasicCall.getOperator();
            String dbFunName = operator.getName();
            if (operator instanceof SqlBetweenOperator) {
                final SqlNode sqlNode = dbFunBasicCall.getOperandList().get(0);
                if (sqlNode instanceof SqlBasicCall) {
                    final SqlBasicCall sqlNode1 = (SqlBasicCall) sqlNode;
                    dbFunName = sqlNode1.getOperator().getName();
                    paramNames.add(((SqlIdentifier) sqlNode1.getOperandList().get(0)).getSimple());
                }
            } else {
                final List<SqlNode> operandList = dbFunBasicCall.getOperandList();
                for (int i = 0; i < operandList.size(); i++) {
                    final SqlNode sqlNode = operandList.get(i);
                    if (sqlNode instanceof SqlIdentifier) {
                        final String simple = ((SqlIdentifier) sqlNode).getSimple();
                        paramNames.add(simple);
                    } else if (sqlNode instanceof SqlNumericLiteral) {
                        final Object value = ((SqlNumericLiteral) sqlNode).getValue();
                        if (value instanceof BigDecimal) {
                            paramNames.add(((BigDecimal) value).toPlainString());
                        } else {
                            paramNames.add(value.toString());
                        }
                    }
                }
            }
            dbPartitionByRule.setColExpr(paramNames);
            dbPartitionByRule.setType(PartitionByType.valueOf(dbFunName.toUpperCase()));
            dbPartitionOptions.setDbpartitionBy(dbPartitionByRule);
        }

        if (dbPartitions != null) {
            Integer dbCounts = ((SqlLiteral) dbPartitions).intValue(false);
            dbPartitionOptions.setDbpartitions(dbCounts);
        }

        if (tbPartitionBy != null) {
            final List<String> paramNames = new ArrayList<>();
            final SqlBasicCall tbFunBasicCall = (SqlBasicCall) tbPartitionBy;
            final SqlOperator operator = tbFunBasicCall.getOperator();
            String tbFunName = operator.getName();
            if (operator instanceof SqlBetweenOperator) {
                final SqlNode sqlNode = tbFunBasicCall.getOperandList().get(0);
                if (sqlNode instanceof SqlBasicCall) {
                    final SqlBasicCall sqlNode1 = (SqlBasicCall) sqlNode;
                    tbFunName = sqlNode1.getOperator().getName();
                    paramNames.add(((SqlIdentifier) sqlNode1.getOperandList().get(0)).getSimple());
                }
                final List<SqlNode> operandList = tbFunBasicCall.getOperandList();
                assert operandList.size() == 3;
                final SqlNode between = operandList.get(1);
                final SqlNode and = operandList.get(2);
                if (between instanceof SqlNumericLiteral) {
                    final Object value = ((SqlNumericLiteral) between).getValue();
                    if (value instanceof BigDecimal) {
                        dbPartitionOptions.setStartWith(((BigDecimal) value).toBigInteger().intValue());
                    } else {
                        dbPartitionOptions.setStartWith(Integer.valueOf(value.toString()));
                    }
                }

                if (and instanceof SqlNumericLiteral) {
                    final Object value = ((SqlNumericLiteral) and).getValue();
                    if (value instanceof BigDecimal) {
                        dbPartitionOptions.setEndWith(((BigDecimal) value).toBigInteger().intValue());
                    } else {
                        dbPartitionOptions.setEndWith(Integer.valueOf(value.toString()));
                    }
                }
            } else {
                final List<SqlNode> operandList = tbFunBasicCall.getOperandList();
                for (int i = 0; i < operandList.size(); i++) {
                    final SqlNode sqlNode = operandList.get(i);
                    if (sqlNode instanceof SqlIdentifier) {
                        final String simple = ((SqlIdentifier) sqlNode).getSimple();
                        paramNames.add(simple);
                    } else if (sqlNode instanceof SqlNumericLiteral) {
                        final Object value = ((SqlNumericLiteral) sqlNode).getValue();
                        if (value instanceof BigDecimal) {
                            paramNames.add(((BigDecimal) value).toPlainString());
                        } else {
                            paramNames.add(value.toString());
                        }
                    }
                }
            }
            tbPartitionByRule.setColExpr(paramNames);
            tbPartitionByRule.setType(PartitionByType.valueOf(tbFunName));
            dbPartitionOptions.setTbpartitionBy(tbPartitionByRule);
        }

        if (tbPartitions != null) {
            Integer tbCounts = ((SqlLiteral) tbPartitions).intValue(false);
            dbPartitionOptions.setTbpartitions(tbCounts);
        }

        return processDBPartitionOptions(tableName, tableToSchema, dbPartitionOptions, optimizerContext,
            executionContext.isRandomPhyTableEnabled());
    }

    /**
     * Convert processDBPartitionOptions to tableRuleList partitions是总group数
     * subpartitoins是每个group的表数，而不是总表数，这样方便使null和1相等
     */
    private static TableRule processDBPartitionOptions(String tableName, TableMeta tableMeta,
                                                       DBPartitionOptions dbpartitionOptions,
                                                       OptimizerContext optimizerContext,
                                                       boolean randomPhyTableNameEnabled) {
        if (dbpartitionOptions == null) {
            /* 无dbpartition为单库单表 */
            return null;
        }

        DBPartitionBy dbpartitionBy = dbpartitionOptions.getDbpartitionBy();
        TBPartitionBy tbpartitionBy = dbpartitionOptions.getTbpartitionBy();
        List<DBPartitionDefinition> dbpartitionDefinitionList = dbpartitionOptions.getDbpartitionDefinitionList();

        DBPartitionDefinition dbpartitionDefinition = null;
        if (dbpartitionDefinitionList != null && dbpartitionDefinitionList.size() > 0) {
            /* only take item(0) of partitionDefinitionList */
            dbpartitionDefinition = dbpartitionDefinitionList.get(0);
        }
        TBPartitionDefinition tbpartitionDefinition = null;
        if (dbpartitionDefinition != null && dbpartitionDefinition.getTbpartitionDefinitionList() != null
            && dbpartitionDefinition.getTbpartitionDefinitionList().size() > 0) {
            /* only take item(0) of SubpartitionDefinitionList */
            tbpartitionDefinition = dbpartitionDefinition.getTbpartitionDefinitionList().get(0);
        }

        TableRule tableRule = new TableRule();
        boolean needCheckTable = true;
        /**
         * 判断partition by,如果为NULL，则没有规则，而是SQL直接下推到缺省DB Statement
         * node的PartitionBy作为后面是否存在partition部分的直接依据，也就是说不允许
         * 没有Partition的Subpartition关键字的形式。
         */
        if (dbpartitionBy == null && tbpartitionBy == null) {
            return null;
        }

        tableRule.setRandomTableNamePatternEnabled(randomPhyTableNameEnabled);

        /**
         * 分库分表数处理 group数目 没有设置的时候则自动处理， 如果设置成0则报错 tablePerGroup数目
         * 没有设置的时候是1，如果设置成0则报错
         */
        int group_count;
        if (dbpartitionOptions.getDbpartitions() == null) {
            if (dbpartitionBy == null) {
                /**
                 * 只分表不分库
                 */
                group_count = 1;
            } else {
                /**
                 * 正常的分库, 只是没写dbpartitions的情况
                 */
                group_count = 0; /* 后面会自动进行替换 */
            }
        } else {
            if (dbpartitionOptions.getDbpartitions() < 1) {
                throw new IllegalArgumentException("dbpartitions should > 0");
            }
            group_count = dbpartitionOptions.getDbpartitions(); // 分库数
        }

        int table_count_on_each_group;
        if (dbpartitionOptions.getStartWith() != null || dbpartitionOptions.getEndWith() != null) {
            table_count_on_each_group = 2;
            needCheckTable = false;
            // 对于noloop版本的spe time, 分表的创建由指定的范围决定, 在规则计算之前不确定每个分库要创建多少张分表
            // 每个分库的表数暂时写死为2,
            // 这个逻辑决定着tbpattern，如果为1的话，说明每个分库只创建一张表则默认为逻辑表，所以这里的值要比1大即可
        } else if (dbpartitionOptions.getTbpartitions() == null) {
            table_count_on_each_group = 1;
        } else {
            if (dbpartitionOptions.getTbpartitions() < 1) {
                throw new IllegalArgumentException("tbpartitions should > 0");
            }
            table_count_on_each_group = dbpartitionOptions.getTbpartitions(); // 每个库的分表数
        }

        if (dbpartitionBy != null && dbpartitionBy.getColExpr().size() == 0) {
            throw new IllegalArgumentException("Can't set dbpartition key to empty!");
        }

        if (tbpartitionBy != null && tbpartitionBy.getColExpr().size() == 0) {
            throw new IllegalArgumentException("Can't set tbpartition key to empty!");
        }

        if (dbpartitionBy != null
            && (dbpartitionBy.getType() == PartitionByType.MM || dbpartitionBy.getType() == PartitionByType.DD
            || dbpartitionBy.getType() == PartitionByType.WEEK || dbpartitionBy.getType() == PartitionByType.MMDD)) {
            throw new IllegalArgumentException("Not support dbpartition method date");
        }

        /* 获得所有真实group的列表 */
        List<Group> dbList = optimizerContext.getMatrix().getGroups();

        /* 用于检查 */
        List<String> selGroupList = new ArrayList<>();

        /* 计算缺省的dbname pattern */
        if (dbpartitionDefinition == null || dbpartitionDefinition.getPartition_name() == null) {
            /* 没有指定partition的形式的时候，自动生成 */
            String defaultDb = optimizerContext.getRuleManager().getDefaultDbIndex(null);

            String dbNamePattern = RuleUtils.genDBPatitionDefinition(group_count,
                table_count_on_each_group,
                dbList,
                defaultDb,
                selGroupList);

            if (group_count == 0) {
                /* 根据自动推断取得最大相似的group大小 */
                group_count = selGroupList.size();
                if (group_count == 0) {
                    /* 如果group数没有指定,则设为默认8 */
                    group_count = 8;
                }
            }

            dbpartitionDefinition = new DBPartitionDefinition();
            dbpartitionDefinition.setPartition_name(SqlLiteral.createCharString(dbNamePattern, SqlParserPos.ZERO));
            dbpartitionDefinition.setStartWith(dbpartitionOptions.getStartWith());
            dbpartitionDefinition.setEndWith(dbpartitionOptions.getEndWith());

            tbpartitionDefinition = new TBPartitionDefinition();
            tbpartitionDefinition.setStartWith(dbpartitionOptions.getStartWith());
            tbpartitionDefinition.setEndWith(dbpartitionOptions.getEndWith());
        } else {
            /**
             * 如果已经设置了partition占位符，就需要根据占位符挑出合适的组，因为后面会对组进行合法性检测,
             * 这里直接将所有的物理group加到选择的列表中，只要选择的group能包含所有的推演出的group就代表OK
             */
            selGroupList.addAll(RuleUtils.groupToStringList(dbList));
        }

        /* 处理分库，分库dbNamePattern一定不为null */
        /* 但可以不是分库 */
        IPartitionGen partitionGen = TableRuleGenFactory.getInstance()
            .createDBPartitionGenerator(optimizerContext.getSchemaName(), dbpartitionBy == null ? PartitionByType.HASH :
                dbpartitionBy.getType());

        /**
         * <pre>
         * 分库分表键相同，并且方法也相同，使用全局唯一的分表表名
         *
         * 如果分库分表键不同，包括分表用时间都使用单独的分库和分表描述方式，分表在每个分库中唯一，分表表名跨库重复
         * </pre>
         */
        PartitionByType tbPartitionType;
        List<String> tbPartitionColExpr;
        if (tbpartitionBy == null) {

            /**
             * DDL中没有指定分表键(即tbPartition)，则是分库不分表，
             *
             * <pre>
             * 这时默认分库分表键相同, 使用useStandAlone=false模式;
             * ， 因为如果这种情况下，默认分库分表键不相同，那么库表就会独立枚举，这里每个库的表名就会变成
             *
             *  xxx_tbl_0, 而不是xxx_tbl.
             * </pre>
             */
            tbPartitionType = dbpartitionBy.getType();
            tbPartitionColExpr = dbpartitionBy.getColExpr();
        } else {
            tbPartitionType = tbpartitionBy.getType();
            tbPartitionColExpr = tbpartitionBy.getColExpr();
        }

        ISubpartitionGen subpartitionGen = TableRuleGenFactory.getInstance()
            .createTBPartitionGenerator(optimizerContext.getSchemaName(), dbpartitionBy == null ? PartitionByType.HASH :
                    dbpartitionBy.getType(),
                tbPartitionType);

        /**
         * 默认使用全局唯一的方式生成规则
         */
        boolean useStandAlone = false;
        List<String> dbPartitionParams = new ArrayList<>();
        List<String> tbPartitionParams = new ArrayList<>();
        if (dbpartitionBy != null) {
            dbPartitionParams = dbpartitionBy.getColExpr();
        }
        if (tbpartitionBy != null) {
            tbPartitionParams = tbpartitionBy.getColExpr();
        }

        if (!comparePartitionParamList(dbPartitionParams, tbPartitionParams)) {
            /* 分库分表键不同 */
            useStandAlone = true;
        } else if (dbPartitionParams.isEmpty() || tbPartitionParams.isEmpty()) {
            useStandAlone = true;
        } else {

            /* 分库分表键相同 */
            if (dbpartitionBy.getType() != tbPartitionType) {
                /* 分表键相同，但方式不同，也需要借用分库分表键不同的形式生成唯一的分表名 */
                useStandAlone = true;
            }
            /* 或者其中有空字符串时，直接用原始逻辑表名当物理表名 */
        }

        // For recovery of CREATE TABLE.
        populateExistingRandomSuffix(tableName, tableRule, optimizerContext, randomPhyTableNameEnabled);

        if (useStandAlone) {
            /**
             * 使用独立方式生成分库分表规则
             */

            partitionGen.fillDbRuleStandAlone(tableRule,
                dbPartitionParams,
                tableMeta,
                group_count,
                table_count_on_each_group,
                dbpartitionDefinition);

            subpartitionGen.fillTbRuleStandAlone(tableRule,
                tbPartitionParams,
                tableMeta,
                group_count,
                table_count_on_each_group,
                tableName,
                tbpartitionDefinition);
        } else {
            /**
             * 使用全局唯一方式生成分库分表规则
             */

            partitionGen.fillDbRule(tableRule, dbPartitionParams, // name
                tableMeta,
                group_count, // 分库数
                table_count_on_each_group, // 每库分表数
                dbpartitionDefinition /* dbNamePattern */);

            subpartitionGen.fillTbRule(tableRule, tbPartitionParams, // name
                tableMeta,
                group_count, // 分库数
                table_count_on_each_group, // 每个库的物理表数
                tableName,
                tbpartitionDefinition /* tbNamePattern */);
        }

        if (dbpartitionBy != null && tbpartitionBy != null && dbpartitionBy.getType() != null
            && tbpartitionBy.getType() != null) {
            if (dbpartitionBy.getType().canCoverRule() && tbpartitionBy.getType().canCoverRule()) {
                tableRule.setCoverRule(true);
            }
        }

        /* 总是加上allowfulltablescan */
        tableRule.setAllowFullTableScan(true);

        /**
         * 这里可以直接做规则推导,因为来源是从SQL来的不存在 并发竞争的问题.
         */
        tableRule.init();

        ShardFuncParamsChecker.validateShardFuncionForTableRule(tableRule);

        /**
         * 这里特别的有一种情况，因为前面是通过真实的group列表来生成tableRule的
         * 但这里是通过这个抽象的tableRule来枚举出计算出的group列表的，这就要求
         * 真实的group必须是连续的，否则如果真实的是0,1,3,4，我这里计算的结果则会是
         * 0,1,2,3就会出错，而且这种错误是在执行的时候才会发现的，而且即使我在这里可以将
         * 真实的groupList带过来，但是之行的时候还是会shard到不存在的group中，这样还不如在
         * 建库的时候直接报错比较好，所以此处需要做预先校验工作
         */
        Map<String, Set<String>> topology = tableRule.getActualTopology();

        /* 只要选择的group能包含所有的推演出的group就代表OK */
        if (!RuleUtils.checkIfGroupMatch(new HashSet<String>(selGroupList), topology.keySet())) {
            throw new IllegalArgumentException("Selected physical group list is invalid:" + selGroupList);
        }

        /**
         * 因为前面的各种方式目的就是保证所有分表编号全局唯一，所以 这里进行最后的检查
         */
        String dbRule = (tableRule.getDbRuleStrs() == null || tableRule.getDbRuleStrs().length == 0) ? null :
            tableRule.getDbRuleStrs()[0];
        String tbRule = (tableRule.getTbRulesStrs() == null || tableRule.getTbRulesStrs().length == 0) ? null :
            tableRule.getTbRulesStrs()[0];

        /**
         * 检查最终生成的分表数与期望分表数是否相同
         */
        if (!RuleUtils.checkIfTableNumberOk(topology, group_count, table_count_on_each_group) && needCheckTable) {

            int actualTbSize = topology.values().iterator().next().size();
            String errorMsg = String.format(
                "The params of ddl may be invalid, because the expected physical tables number for each group specified by ddl mismatch the actual generated number, the expected/actual number is [%s/%s]",
                table_count_on_each_group, actualTbSize);
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errorMsg);

        }

        return tableRule;
    }

    public static TableRule buildBroadcastTableRule(String tableName, TableMeta tableMeta) {
        return buildBroadcastTableRule(tableName, tableMeta, OptimizerContext.getContext(tableMeta.getSchemaName()),
            true);
    }

    public static TableRule buildBroadcastTableRuleWithoutRandomPhyTableName(String tableName, TableMeta tableMeta) {
        return buildBroadcastTableRule(tableName, tableMeta, OptimizerContext.getContext(tableMeta.getSchemaName()),
            false);
    }

    public static TableRule buildBroadcastTableRule(String tableName, TableMeta tableMeta,
                                                    OptimizerContext optimizerContext,
                                                    boolean randomPhyTableNameEnabled) {
        TableRule tableRule = new TableRule();

        tableRule.setBroadcast(true);
        tableRule.setRandomTableNamePatternEnabled(randomPhyTableNameEnabled);

        int table_count_on_each_group = 1; /* 一定单表 */
        int group_count = 1; /* 广播规则生成只需要1 */

        String defaultDb = optimizerContext.getRuleManager().getDefaultDbIndex(null);

        /* 用于检查 */
        List<String> selGroupList = new ArrayList<>();
        tableRule.setDbNamePattern(defaultDb);
        /* 这里需要跳过后面的检测 */
        selGroupList.add(defaultDb);

        TableRuleUtil.populateExistingRandomSuffix(tableName, tableRule, optimizerContext, randomPhyTableNameEnabled);

        /* 借用HASH的partition/subpartition生成法则 */
        IPartitionGen partitionGen =
            TableRuleGenFactory.getInstance().createDBPartitionGenerator(optimizerContext.getSchemaName(),
                PartitionByType.HASH);
        ISubpartitionGen subpartitionGen = TableRuleGenFactory.getInstance()
            .createTBPartitionGenerator(optimizerContext.getSchemaName(), PartitionByType.HASH, PartitionByType.HASH);

        List<String> partitionParams = new ArrayList<>();
        partitionGen.fillDbRuleStandAlone(tableRule, partitionParams, // 分库键
            tableMeta,
            group_count, // 分库数
            1,
            null);

        subpartitionGen.fillTbRuleStandAlone(tableRule, partitionParams, // 分表键
            tableMeta,
            group_count,
            table_count_on_each_group,
            tableName,
            null);

        /* 总是加上allowfulltablescan */
        tableRule.setAllowFullTableScan(true);

        /**
         * 这里可以直接做规则推导,因为来源是从SQL来的不存在 并发竞争的问题.
         */
        tableRule.init();

        /**
         * 这里特别的有一种情况，因为前面是通过真实的group列表来生成tableRule的
         * 但这里是通过这个抽象的tableRule来枚举出计算出的group列表的，这就要求
         * 真实的group必须是连续的，否则如果真实的是0,1,3,4，我这里计算的结果则会是
         * 0,1,2,3就会出错，而且这种错误是在执行的时候才会发现的，而且即使我在这里可以将
         * 真实的groupList带过来，但是之行的时候还是会shard到不存在的group中，这样还不如在
         * 建库的时候直接报错比较好，所以此处需要做预先校验工作
         */
        Map<String, Set<String>> topology = tableRule.getActualTopology();

        /* 只要选择的group能包含所有的推演出的group就代表OK */
        if (!RuleUtils.checkIfGroupMatch(new HashSet<>(selGroupList), topology.keySet())) {
            throw new IllegalArgumentException("Selected physical group list is invalid:" + selGroupList);
        }

        /**
         * 因为前面的各种方式目的就是保证所有分表编号全局唯一，所以 这里进行最后的检查
         */
        String dbRule = (tableRule.getDbRuleStrs() == null || tableRule.getDbRuleStrs().length == 0) ? null :
            tableRule.getDbRuleStrs()[0];
        String tbRule = (tableRule.getTbRulesStrs() == null || tableRule.getTbRulesStrs().length == 0) ? null :
            tableRule.getTbRulesStrs()[0];

        /**
         * 检查最终生成的分表数与期望分表数是否相同
         */
        if (!RuleUtils.checkIfTableNumberOk(topology, group_count, table_count_on_each_group)) {
            throw new IllegalArgumentException("Generated table number mismatch" + " dbName: "
                + tableRule.getDbNamePattern() + " tbName: "
                + tableRule.getTbNamePattern() + " dbRule: " + dbRule + " tbRule: "
                + tbRule);
        }

        return tableRule;
    }

    /**
     * 比较两个Partition的参数列表是否完全一致
     *
     * @return 返回true 表示完全一致；否则为不一致
     */
    private static boolean comparePartitionParamList(List<String> paramList1, List<String> paramList2) {
        if (paramList1 == null && paramList2 != null) {
            return false;
        }

        if (paramList1 != null && paramList2 == null) {
            return false;
        }

        if (paramList1 == null && paramList2 == null) {
            return true;
        }

        if (paramList1.size() != paramList2.size()) {
            return false;
        }

        int paramCount = paramList1.size();
        for (int i = 0; i < paramCount; i++) {
            if (!paramList1.get(i).equals(paramList2.get(i))) {
                return false;
            }
        }

        return true;
    }

    public static void populateExistingRandomSuffix(String tableName, TableRule tableRule,
                                                    OptimizerContext optimizerContext,
                                                    boolean randomPhyTableNameEnabled) {
        try {
            TddlRule tddlRule = optimizerContext.getRuleManager().getTddlRule();
            if (tddlRule != null && randomPhyTableNameEnabled) {
                TableRule existingTableRule = optimizerContext.getRuleManager().getTableRule(tableName);
                if (existingTableRule != null) {
                    String randomSuffix = existingTableRule.getExistingRandomSuffixForRecovery();
                    if (TStringUtil.isEmpty(randomSuffix)) {
                        randomSuffix = existingTableRule.extractRandomSuffix();
                    }
                    if (TStringUtil.isNotEmpty(randomSuffix)) {
                        tableRule.setExistingRandomSuffixForRecovery(randomSuffix);
                    }
                } else {
                    String tableNamePrefix = tddlRule.getTableNamePrefixForShadowTable(tableName);
                    if (TStringUtil.isNotEmpty(tableNamePrefix)) {
                        tableRule.setTableNamePrefixForShadowTable(tableNamePrefix);
                    }
                }
            }
        } catch (Throwable t) {
            // Ignored
        }
    }

}
