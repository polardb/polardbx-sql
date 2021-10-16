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
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.parse.bean.TBPartitionDefinition;
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
public class SubpartitionGen implements ISubpartitionGen {

    private static char[] illegalChars = new char[] {'{', '}', '$', '#', ',', '\\', ';', '\'', '"'};

    private final String schemaName;

    private PartitionByType tbMethod;

    public SubpartitionGen(String schemaName, PartitionByType dbMethod, PartitionByType tbMethod) {
        this.schemaName = schemaName;
        this.tbMethod = tbMethod;
    }

    /**
     * 填写TableRule的tb部分 tbCol可指定hash多种类型的col或者 mm,dd等日期类型的Column
     */
    @Override
    public void fillTbRule(TableRule tableRule, List<String> partitionParams, TableMeta tableMeta, int group_count,
                           int table_count_on_each_group, String tableName,
                           TBPartitionDefinition tbPartitionDefinition) {

        int containsPlaceHolder;

        /**
         * 没有手工设置tbNamePattern，需要自动生成，
         * 特殊处理:对于分库不分表的情形要保持与console一致的与原表名相同的逻辑而不用保证全局唯一
         */
        String tableNameWithRandomSuffix;
        if (table_count_on_each_group == 1) {
            // tablename 已经是转义后的了
            if (group_count > 1 && tableRule.isRandomTableNamePatternEnabled()) {
                tableNameWithRandomSuffix = RuleUtils.genTableNameWithRandomSuffix(tableRule, tableName);
                tableRule.setTbNamePattern(tableNameWithRandomSuffix);
            } else {
                tableRule.setTbNamePattern(tableName);
            }
            containsPlaceHolder = 0;
        } else {
            if (tableRule.isRandomTableNamePatternEnabled()) {
                tableNameWithRandomSuffix = RuleUtils.genTableNameWithRandomSuffix(tableRule,
                    tableName,
                    group_count,
                    table_count_on_each_group);
            } else {
                tableNameWithRandomSuffix = tableName;
            }

            String tbNamePattern = RuleUtils.genTBPartitionDefinition(tableNameWithRandomSuffix,
                group_count,
                table_count_on_each_group);

            tableRule.setTbNamePattern(tbNamePattern);

            containsPlaceHolder = containMultiPlaceHolder(tbNamePattern);
        }

        if (table_count_on_each_group == 1 && containsPlaceHolder == 0) {
            /* 单表时且没有占位符则不用设置tbRule */
            return;
        } else if (table_count_on_each_group > 1 && containsPlaceHolder != 1) {
            /* 指定分表数目大于1但没有占位符报错 */
            throw new IllegalArgumentException("tbpartitions > 1 but no placeholder for tbNamePattern");
        }

        boolean isNewShardFunc = PartitionByTypeUtils.isNewShardFunctionTypeByType(tbMethod);
        if (!isNewShardFunc) {

            String shardKey = partitionParams.get(0);

            // String tbKeyStr = RuleUtils.aliasUnescapeUppercase(shardKey,
            // false);

            String tbKeyStr = shardKey;

            if (!tbKeyStr.isEmpty()) {
                ColumnMeta columnMeta = tableMeta.getColumn(tbKeyStr);
                if (columnMeta == null) {
                    throw new OptimizerException("not found partition column:" + tbKeyStr);
                }

                if (TStringUtil.containsAny(tbKeyStr, illegalChars)) {
                    throw new OptimizerException("partition column:"
                        + tbKeyStr
                        + " should not contains any illegal chars '{', '}', '$', '#', ',', '\', ';'");
                }
                IRuleGen tableRuleGen =
                    TableRuleGenFactory.getInstance().createRuleGenerator(schemaName, columnMeta.getDataType());
                String tblRuleArrayStr = RuleUtils.aliasUnescapeUppercase(tableRuleGen.generateTbRuleArrayStr(tbMethod,
                    tbKeyStr,
                    1,
                    group_count,
                    table_count_on_each_group,
                    tbPartitionDefinition), false);
                List<String> tblRules = new LinkedList<String>();
                tblRules.add(tblRuleArrayStr);
                tableRule.setTbRuleArray(tblRules);
            }
        } else {
            List<String> paramList = partitionParams;
            List<Class> paramTypes = buildParamTypes(tableMeta, paramList);

            ShardFunctionMeta tbShardFunctionMeta =
                ShardFunctionMetaFactory.getShardFunctionMetaByPartitionByType(tbMethod.toString());

            tbShardFunctionMeta.setDbCount(group_count);
            tbShardFunctionMeta.setTbCountEachDb(table_count_on_each_group);
            tbShardFunctionMeta.setTbStandAlone(false);
            tbShardFunctionMeta.setShardType(ShardFunctionMeta.SHARD_TYPE_TB);
            tbShardFunctionMeta.setParamType(paramTypes);
            tbShardFunctionMeta.initFunctionParams(paramList);
            ShardFuncParamsChecker.checkColmunMeta(tableMeta, tbMethod, tbShardFunctionMeta);

            List<String> tbRuleArrayStr = tbShardFunctionMeta.buildRuleShardFunctionStr();
            tableRule.setTbRuleArray(tbRuleArrayStr);
            tableRule.setTbShardFunctionMeta(tbShardFunctionMeta);
        }

    }

    private List<Class> buildParamTypes(TableMeta tableMeta, List<String> paramList) {
        List<Class> paramTypes = new LinkedList<Class>();
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

    /**
     * 分表数目自己维护，每库唯一，跨库可重复
     */
    @Override
    public void fillTbRuleStandAlone(TableRule tableRule, List<String> partitionParams, TableMeta tableMeta,
                                     int group_count, int table_count_on_each_group, String tableName,
                                     TBPartitionDefinition tbPartitionDefinition) {

        int containsPlaceHolder;
        String tableNameWithRandomSuffix;
        if (partitionParams.isEmpty()) {
            /* 如果没有设置分表键，则只能允许分表数为1 */
            if (table_count_on_each_group > 1) {
                throw new IllegalArgumentException("tbpartitions should not > 1 when no tbCol specifed");
            }
            if (tableRule.isRandomTableNamePatternEnabled()) {
                tableNameWithRandomSuffix = RuleUtils.genTableNameWithRandomSuffix(tableRule, tableName);
                tableRule.setTbNamePattern(tableNameWithRandomSuffix);
            } else {
                tableRule.setTbNamePattern(tableName);
            }
            containsPlaceHolder = 0;
        } else {
            /**
             * 没有手工设置tbNamePattern，需要自动生成
             */
            if (tableRule.isRandomTableNamePatternEnabled()) {
                tableNameWithRandomSuffix = RuleUtils.genTableNameWithRandomSuffix(tableRule,
                    tableName,
                    table_count_on_each_group);
            } else {
                tableNameWithRandomSuffix = tableName;
            }

            String tbNamePattern = RuleUtils.genStandAloneTBPartitionDefinition(tableNameWithRandomSuffix,
                table_count_on_each_group);

            tableRule.setTbNamePattern(tbNamePattern);

            containsPlaceHolder = containMultiPlaceHolder(tbNamePattern);
        }

        if (table_count_on_each_group == 1 && containsPlaceHolder == 0) {
            /* 单表时且没有占位符则不用设置tbRule */
            return;
        } else if (table_count_on_each_group > 1 && containsPlaceHolder != 1) {
            /* 指定分表数目大于1但没有占位符报错 */
            throw new IllegalArgumentException("tbpartitions > 1 but no placeholder for tbNamePattern");
        }

        boolean isNewShardFunc = PartitionByTypeUtils.isNewShardFunctionTypeByType(tbMethod);

        if (!isNewShardFunc) {

            if (!partitionParams.isEmpty()) {

                String tbKey = partitionParams.get(0);
                ColumnMeta tbColumnMeta = tableMeta.getColumn(tbKey);
                if (tbColumnMeta == null) {
                    throw new OptimizerException("not found partition column:" + tbKey);
                }

                if (TStringUtil.containsAny(tbKey, illegalChars)) {
                    throw new OptimizerException("partition column:"
                        + tbKey
                        + " should not contains any illegal chars '{', '}', '$', '#', ',', '\', ';'");
                }

                IRuleGen tbRuleGen =
                    TableRuleGenFactory.getInstance().createRuleGenerator(schemaName, tbColumnMeta.getDataType());

                /**
                 * Generate two dimension style of table rule
                 */
                String tblRuleArrayStr =
                    RuleUtils.aliasUnescapeUppercase(tbRuleGen.generateStandAloneTbRuleArrayStr(tbMethod,
                        tbKey,
                        1,
                        table_count_on_each_group,
                        tbPartitionDefinition),
                        false);

                List<String> tblRules = new LinkedList<String>();
                tblRules.add(tblRuleArrayStr);
                tableRule.setTbRuleArray(tblRules);
            }
        } else {
            List<String> paramList = partitionParams;
            List<Class> paramTypes = buildParamTypes(tableMeta, paramList);

            ShardFunctionMeta tbShardFunctionMeta =
                ShardFunctionMetaFactory.getShardFunctionMetaByPartitionByType(tbMethod.toString());

            tbShardFunctionMeta.setDbCount(group_count);
            tbShardFunctionMeta.setTbCountEachDb(table_count_on_each_group);
            tbShardFunctionMeta.setTbStandAlone(true);
            tbShardFunctionMeta.setShardType(ShardFunctionMeta.SHARD_TYPE_TB);
            tbShardFunctionMeta.setParamType(paramTypes);
            tbShardFunctionMeta.initFunctionParams(paramList);
            ShardFuncParamsChecker.checkColmunMeta(tableMeta, tbMethod, tbShardFunctionMeta);

            List<String> tbRuleArrayStr = tbShardFunctionMeta.buildRuleShardFunctionStr();
            tableRule.setTbRuleArray(tbRuleArrayStr);
            tableRule.setTbShardFunctionMeta(tbShardFunctionMeta);

        }

    }

    /**
     * 查找是否包含多个{}标识
     */
    private int containMultiPlaceHolder(SqlLiteral name) {
        return containMultiPlaceHolder(name.getStringValue());
    }

    private int containMultiPlaceHolder(String name) {
        int first_left_pos = name.indexOf('{');
        int last_left_pos = name.lastIndexOf('{');

        int first_right_pos = name.indexOf('}');
        int last_right_pos = name.lastIndexOf('}');

        if ((first_left_pos != -1 && last_left_pos != -1 && first_left_pos != last_left_pos
            && first_right_pos > first_left_pos)
            && (first_right_pos != -1 && last_right_pos != -1 && first_right_pos != last_right_pos
            && last_right_pos > last_left_pos)) {
            return 2;
        } else if (first_left_pos != -1 && first_right_pos != -1) {
            return 1;
        }

        return 0;
    }

}
