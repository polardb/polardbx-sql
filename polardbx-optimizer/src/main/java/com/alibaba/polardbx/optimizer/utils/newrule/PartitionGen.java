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

package com.alibaba.polardbx.optimizer.utils.newrule;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionDefinition;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.ddl.PartitionByType;
import com.alibaba.polardbx.rule.ddl.PartitionByTypeUtils;
import com.alibaba.polardbx.rule.meta.ShardFunctionMeta;
import com.alibaba.polardbx.rule.meta.ShardFunctionMetaFactory;
import org.apache.calcite.sql.SqlLiteral;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by simiao on 15-6-4.
 */
public class PartitionGen implements IPartitionGen {

    public static char[] illegalChars = new char[] {'{', '}', '$', '#', ',', '\\', ';', '\'', '"'};

    private final String schemaName;

    private PartitionByType dbMethod;

    public PartitionGen(String schemaName, PartitionByType dbMethod) {
        this.schemaName = schemaName;
        this.dbMethod = dbMethod;
    }

    /**
     * <pre>
     * 填写TableRule的db部分 hash包含是string的hash和integer的hash两种
     * 分库按照HASH(name)，生成的
     *      dbRuleArray 为(#name,1, tablecount#.hashcode().abs()% tablecount).intdiv(tablecount/groupcount)
     * </pre>
     *
     * @param partitionParams 可以为空，表示不生成dbRuleArray
     */
    @Override
    public void fillDbRule(TableRule tableRule, List<String> partitionParams, TableMeta tableMeta,
                           int group_count,
                           int tablesPerGroup, DBPartitionDefinition dbPartitionDefinition) {

        if (dbPartitionDefinition != null && dbPartitionDefinition.getPartition_name() != null) {
            /* 将来允许(partition partion_name)没有，这样仅通过dbRuleArray来推出数据库名称 */
            if (!(dbPartitionDefinition.getPartition_name() instanceof SqlLiteral)) {
                throw new IllegalArgumentException("partition name must be string!");
            }

            SqlLiteral partition_name = (SqlLiteral) dbPartitionDefinition.getPartition_name();

            // dbNamePattern
            tableRule.setDbNamePattern(RuleUtils.aliasUnescapeUppercase(partition_name.getStringValue(), false));
        }

        if (group_count == 1) {
            /* 如果只有一个库则不需要写规则了 */
            return;
        }

        boolean isNewShardFunc = PartitionByTypeUtils.isNewShardFunctionTypeByType(dbMethod);
        if (!isNewShardFunc) {

            if (partitionParams.isEmpty()) {
                throw new IllegalArgumentException("partition function no found shard key.");
            }

            String shardKey = partitionParams.get(0);

            // dbRuleArray
            // String dbKeyStr = RuleUtils.aliasUnescapeUppercase(shardKey,
            // false);
            String dbKeyStr = shardKey;
            if (!dbKeyStr.isEmpty()) {
                /**
                 * 只有当dateKey有内容时才会生成dbRuleArray
                 */
                ColumnMeta columnMeta = tableMeta.getColumn(dbKeyStr);
                if (columnMeta == null) {
                    throw new OptimizerException("not found partition column:" + dbKeyStr);
                }

                if (TStringUtil.containsAny(dbKeyStr, illegalChars)) {
                    throw new OptimizerException("partition column:"
                        + dbKeyStr
                        + " should not contains any illegal chars '{', '}', '$', '#', ',', '\', ';'");
                }

                IRuleGen tableRuleGen = TableRuleGenFactory.getInstance().createRuleGenerator(schemaName,
                    columnMeta.getDataType());

                /**
                 * 针对分库 分库数都是按照真实的能匹配的最大匹配模式的分库数决定的，所以数目不受自然月的影响 但写法需要改变
                 */
                String dbRuleArrayStr = RuleUtils.aliasUnescapeUppercase(tableRuleGen.generateDbRuleArrayStr(dbMethod,
                    dbKeyStr,
                    1,
                    group_count,
                    tablesPerGroup, dbPartitionDefinition), false);

                List<String> dbRules = new LinkedList<String>();
                dbRules.add(dbRuleArrayStr);
                tableRule.setDbRuleArray(dbRules);
            }
        } else {
            List<String> paramList = partitionParams;

            List<Class> paramTypes = buildParamTypes(tableMeta, paramList);

            ShardFunctionMeta dbShardFunctionMeta =
                ShardFunctionMetaFactory.getShardFunctionMetaByPartitionByType(dbMethod.toString());

            dbShardFunctionMeta.setDbCount(group_count);
            dbShardFunctionMeta.setTbCountEachDb(tablesPerGroup);
            dbShardFunctionMeta.setTbStandAlone(false);
            dbShardFunctionMeta.setShardType(ShardFunctionMeta.SHARD_TYPE_DB);
            dbShardFunctionMeta.setParamType(paramTypes);
            dbShardFunctionMeta.initFunctionParams(paramList);
            ShardFuncParamsChecker.checkColmunMeta(tableMeta, dbMethod, dbShardFunctionMeta);

            List<String> dbRuleArrayStr = dbShardFunctionMeta.buildRuleShardFunctionStr();

            tableRule.setDbRuleArray(dbRuleArrayStr);
            tableRule.setDbShardFunctionMeta(dbShardFunctionMeta);
        }

    }

    private List<Class> buildParamTypes(TableMeta tableMeta, List<String> paramList) {
        List<Class> paramTypes = new LinkedList<>();
        for (String param : paramList) {
            /**
             * 目前先只支持 String 和 Number 的自定义类型。
             */
            ColumnMeta column = tableMeta.getColumn(param);
            if (column == null) {
                paramTypes.add(null);
                continue;
            }
            DataType dataType = column.getDataType();
            if (DataTypeUtil.isNumberSqlType(dataType)) {
                paramTypes.add(Number.class);
            } else if (DataTypeUtil.isStringType(dataType)) {
                paramTypes.add(String.class);
            }
        }
        return paramTypes;
    }

    @Override
    public void fillDbRuleStandAlone(TableRule tableRule, List<String> partitionParams, TableMeta tableMeta,
                                     int group_count, int tablesPerGroup, DBPartitionDefinition dbPartitionDefinition) {

        if (dbPartitionDefinition != null && dbPartitionDefinition.getPartition_name() != null) {
            /* 将来允许(partition partion_name)没有，这样仅通过dbRuleArray来推出数据库名称 */
            if (!(dbPartitionDefinition.getPartition_name() instanceof SqlLiteral)) {
                throw new IllegalArgumentException("partition name must be string!");
            }

            SqlLiteral partition_name = (SqlLiteral) dbPartitionDefinition.getPartition_name();

            // dbNamePattern
            tableRule.setDbNamePattern(RuleUtils.aliasUnescapeUppercase(partition_name.getStringValue(), false));
        }

        if (group_count == 1) {
            /* 如果只有一个库则不需要写规则了 */
            return;
        }

        boolean isNewShardFunc = PartitionByTypeUtils.isNewShardFunctionTypeByType(dbMethod);
        if (!isNewShardFunc) {

            if (partitionParams.isEmpty()) {
                throw new IllegalArgumentException("partition function no found shard key.");
            }

            String shardKey = partitionParams.get(0);

            // dbRuleArray
            // String dbKeyStr = RuleUtils.aliasUnescapeUppercase(shardKey,
            // false);
            String dbKeyStr = shardKey;
            if (!dbKeyStr.isEmpty()) {
                /**
                 * 只有当dateKey有内容时才会生成dbRuleArray
                 */
                ColumnMeta columnMeta = tableMeta.getColumn(dbKeyStr);
                if (columnMeta == null) {
                    throw new OptimizerException("not found partition column:" + dbKeyStr);
                }

                if (TStringUtil.containsAny(dbKeyStr, illegalChars)) {
                    throw new OptimizerException("partition column:"
                        + dbKeyStr
                        + " should not contains any illegal chars '{', '}', '$', '#', ',', '\', ';'");
                }

                IRuleGen tableRuleGen =
                    TableRuleGenFactory.getInstance().createRuleGenerator(schemaName, columnMeta.getDataType());

                /**
                 * 针对分库 分库数都是按照真实的能匹配的最大匹配模式的分库数决定的，所以数目不受自然月的影响 但写法需要改变
                 */
                String dbRuleArrayStr =
                    RuleUtils.aliasUnescapeUppercase(tableRuleGen.generateStandAloneDbRuleArrayStr(dbMethod,
                        dbKeyStr,
                        1,
                        group_count, dbPartitionDefinition),
                        false);

                List<String> dbRules = new LinkedList<String>();
                dbRules.add(dbRuleArrayStr);
                tableRule.setDbRuleArray(dbRules);
            }
        } else {

            List<String> paramList = partitionParams;
            List<Class> paramTypes = buildParamTypes(tableMeta, paramList);

            ShardFunctionMeta dbShardFunctionMeta =
                ShardFunctionMetaFactory.getShardFunctionMetaByPartitionByType(dbMethod.toString());

            dbShardFunctionMeta.setDbCount(group_count);
            dbShardFunctionMeta.setTbCountEachDb(tablesPerGroup);
            dbShardFunctionMeta.setTbStandAlone(true);
            dbShardFunctionMeta.setShardType(ShardFunctionMeta.SHARD_TYPE_DB);
            dbShardFunctionMeta.setParamType(paramTypes);
            dbShardFunctionMeta.initFunctionParams(paramList);
            ShardFuncParamsChecker.checkColmunMeta(tableMeta, dbMethod, dbShardFunctionMeta);

            List<String> dbRuleArrayStr = dbShardFunctionMeta.buildRuleShardFunctionStr();
            tableRule.setDbRuleArray(dbRuleArrayStr);
            tableRule.setDbShardFunctionMeta(dbShardFunctionMeta);

        }

    }
}
