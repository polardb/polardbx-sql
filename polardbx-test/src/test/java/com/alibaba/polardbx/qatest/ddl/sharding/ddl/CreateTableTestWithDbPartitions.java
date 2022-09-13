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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.RuleValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch 2017年3月13日 上午10:42:49
 * @since 5.0.0
 */

public class CreateTableTestWithDbPartitions extends DDLBaseNewDBTestCase {

    private static String testTableName = "date_test_1";
    private static String testCHNTableName = "中文表名带日期_1";

    protected List<String> testTableNames = new ArrayList<String>();

    protected static Map<String, String> partitiionTestContents = new HashMap<String, String>();

    protected static List<String> shardKeyCols = new ArrayList<String>();

    protected String createSqlModel =
        "CREATE TABLE %s ( `pk` bigint NOT NULL AUTO_INCREMENT COMMENT '', `trade_no` varchar(50) NULL COMMENT '', `date_col` date DEFAULT NULL COMMENT '',  `datetime_col` datetime DEFAULT NULL COMMENT '', `timestamp_col` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '', PRIMARY KEY (`pk`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ";

    protected String partitiionTestType;

    public CreateTableTestWithDbPartitions(String partitiionTestType) {
        this.partitiionTestType = partitiionTestType;
        this.crossSchema = true;

    }

    static {
        partitiionTestContents
            .put("dbAndTbBothUse%s", "dbpartition by %s(`%s`) tbpartition by %s(`%s`) tbpartitions 2");
        partitiionTestContents.put("dbUse%sOnly", "dbpartition by %s(`%s`)");
        partitiionTestContents.put("tbUse%sOnly", "tbpartition by %s(`%s`) tbpartitions 2");
        partitiionTestContents.put("dbAndTbButOnlyDbUse%s",
            "dbpartition by hash(`trade_no`) tbpartition by %s(`%s`) tbpartitions 2");
        partitiionTestContents.put("dbAndTbButOnlyTbUse%s",
            "dbpartition by %s(`%s`) tbpartition by hash(`trade_no`) tbpartitions 2");

        shardKeyCols.add("datetime_col");
        shardKeyCols.add("timestamp_col");
        shardKeyCols.add("date_col");
    }

    @Parameterized.Parameters(name = "{index}:partitiionTestType={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {
                {"dbAndTbBothUse%s"},
                {"dbUse%sOnly"},
                {"tbUse%sOnly"},
                {"dbAndTbButOnlyDbUse%s"},
                {"dbAndTbButOnlyTbUse%s"}
            });
    }

    @Before
    public void init() {
        testTableNames.clear();
        testTableNames.add(schemaPrefix + testTableName);
        testTableNames.add(schemaPrefix + testCHNTableName);
    }

    protected String getPartitiionTest(String type, String col, String fun) {

        String typeContent = partitiionTestContents.get(type);
        String partitions = null;

        if (type.equals("dbAndTbBothUse%s")) {
            partitions = String.format(typeContent, fun, col, fun, col);
        } else if (type.equals("dbUse%sOnly")) {
            partitions = String.format(typeContent, fun, col);
        } else if (type.equals("tbUse%sOnly")) {
            partitions = String.format(typeContent, fun, col);
        } else if (type.equals("dbAndTbButOnlyDbUse%s")) {
            partitions = String.format(typeContent, fun, col);
        } else if (type.equals("dbAndTbButOnlyTbUse%s")) {
            partitions = String.format(typeContent, fun, col);
        }

        return partitions;
    }

    @Test
    public void testPartitionByYYYYMM() {

        String funcName = "YYYYMM";
        for (int k = 0; k < testTableNames.size(); k++) {

            String tableName = testTableNames.get(k);

            String type = partitiionTestType;
            for (int j = 0; j < shardKeyCols.size(); j++) {
                String col = shardKeyCols.get(j);
                String partitionContent = getPartitiionTest(type, col, funcName);

                String typeName = String.format(type, funcName);
                String realTableName = String.format("%s_%s_%s", tableName, typeName, col);
                String createSql = String.format(createSqlModel + partitionContent, realTableName);

                dropTableIfExists(realTableName);
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

                if (type.equals("dbUse%sOnly")) {
                    RuleValidator.assertMuliRuleOnlyWithDbExist(realTableName, tddlConnection);
                } else if (type.equals("tbUse%sOnly")) {
                    RuleValidator.assertMuliRuleOnlyWithTbExist(realTableName, tddlConnection);
                } else {
                    RuleValidator.assertMultiRuleWithBothDbAndTbExist(realTableName, tddlConnection);
                }

                Assert.assertTrue(getShardNum(realTableName) > 0);
                String showCreateTableString = showCreateTable(tddlConnection, realTableName);
                Assert.assertTrue(showCreateTableString.contains(partitionContent));
                dropTableIfExists(realTableName);
            }
        }

    }

    @Test
    public void testPartitionByYYYYMM_OPT() {

        String funcName = "YYYYMM_OPT";
        for (int k = 0; k < testTableNames.size(); k++) {

            String tableName = testTableNames.get(k);

            String type = partitiionTestType;
            for (int j = 0; j < shardKeyCols.size(); j++) {
                String col = shardKeyCols.get(j);
                String partitionContent = getPartitiionTest(type, col, funcName);

                String typeName = String.format(type, funcName);
                String realTableName = String.format("%s_%s_%s", tableName, typeName, col);
                String createSql = String.format(createSqlModel + partitionContent, realTableName);

                dropTableIfExists(realTableName);
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

                if (type.equals("dbUse%sOnly")) {
                    RuleValidator.assertMuliRuleOnlyWithDbExist(realTableName, tddlConnection);
                } else if (type.equals("tbUse%sOnly")) {
                    RuleValidator.assertMuliRuleOnlyWithTbExist(realTableName, tddlConnection);
                } else {
                    RuleValidator.assertMultiRuleWithBothDbAndTbExist(realTableName, tddlConnection);
                }

                Assert.assertTrue(getShardNum(realTableName) > 0);
                String showCreateTableString = showCreateTable(tddlConnection, realTableName);
                Assert.assertTrue(showCreateTableString.contains(partitionContent));
                dropTableIfExists(realTableName);
            }
        }

    }

    @Test
    public void testPartitionByYYYYWEEK() {

        String funcName = "YYYYWEEK";
        for (int k = 0; k < testTableNames.size(); k++) {

            String tableName = testTableNames.get(k);

            String type = partitiionTestType;
            for (int j = 0; j < shardKeyCols.size(); j++) {
                String col = shardKeyCols.get(j);
                String partitionContent = getPartitiionTest(type, col, funcName);

                String typeName = String.format(type, funcName);
                String realTableName = String.format("%s_%s_%s", tableName, typeName, col);
                String createSql = String.format(createSqlModel + partitionContent, realTableName);

                dropTableIfExists(realTableName);
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

                if (type.equals("dbUse%sOnly")) {
                    RuleValidator.assertMuliRuleOnlyWithDbExist(realTableName, tddlConnection);
                } else if (type.equals("tbUse%sOnly")) {
                    RuleValidator.assertMuliRuleOnlyWithTbExist(realTableName, tddlConnection);
                } else {
                    RuleValidator.assertMultiRuleWithBothDbAndTbExist(realTableName, tddlConnection);
                }

                Assert.assertTrue(getShardNum(realTableName) > 0);
                String showCreateTableString = showCreateTable(tddlConnection, realTableName);
                Assert.assertTrue(showCreateTableString.contains(partitionContent));
                dropTableIfExists(realTableName);
            }
        }

    }

    @Ignore("与开发确认不支持")
    @Test
    public void testPartitionByYYYYWEEK_OPT() {

        String funcName = "YYYYWEEK_OPT";
        for (int k = 0; k < testTableNames.size(); k++) {

            String tableName = testTableNames.get(k);

            String type = partitiionTestType;
            for (int j = 0; j < shardKeyCols.size(); j++) {
                String col = shardKeyCols.get(j);
                String partitionContent = getPartitiionTest(type, col, funcName);

                String typeName = String.format(type, funcName);
                String realTableName = String.format("%s_%s_%s", tableName, typeName, col);
                String createSql = String.format(createSqlModel + partitionContent, realTableName);

                dropTableIfExists(realTableName);
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

                if (type.equals("dbUse%sOnly")) {
                    RuleValidator.assertMuliRuleOnlyWithDbExist(realTableName, tddlConnection);
                } else if (type.equals("tbUse%sOnly")) {
                    RuleValidator.assertMuliRuleOnlyWithTbExist(realTableName, tddlConnection);
                } else {
                    RuleValidator.assertMultiRuleWithBothDbAndTbExist(realTableName, tddlConnection);
                }

                Assert.assertTrue(getShardNum(realTableName) > 0);
                String showCreateTableString = showCreateTable(tddlConnection, realTableName);
                Assert.assertTrue(showCreateTableString.contains(partitionContent));
                dropTableIfExists(realTableName);
            }

        }

    }

    @Test
    public void testPartitionByYYYYDD() {

        String funcName = "YYYYDD";
        for (int k = 0; k < testTableNames.size(); k++) {

            String tableName = testTableNames.get(k);

            String type = partitiionTestType;
            for (int j = 0; j < shardKeyCols.size(); j++) {
                String col = shardKeyCols.get(j);
                String partitionContent = getPartitiionTest(type, col, funcName);

                String typeName = String.format(type, funcName);
                String realTableName = String.format("%s_%s_%s", tableName, typeName, col);
                String createSql = String.format(createSqlModel + partitionContent, realTableName);

                dropTableIfExists(realTableName);
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

                if (type.equals("dbUse%sOnly")) {
                    RuleValidator.assertMuliRuleOnlyWithDbExist(realTableName, tddlConnection);
                } else if (type.equals("tbUse%sOnly")) {
                    RuleValidator.assertMuliRuleOnlyWithTbExist(realTableName, tddlConnection);
                } else {
                    RuleValidator.assertMultiRuleWithBothDbAndTbExist(realTableName, tddlConnection);
                }

                Assert.assertTrue(getShardNum(realTableName) > 0);
                String showCreateTableString = showCreateTable(tddlConnection, realTableName);
                Assert.assertTrue(showCreateTableString.contains(partitionContent));
                dropTableIfExists(realTableName);
            }
        }

    }

    @Ignore("与开发确认不支持")
    @Test
    public void testPartitionByYYYYDD_OPT() {

        String funcName = "YYYYDD_OPT";
        for (int k = 0; k < testTableNames.size(); k++) {

            String tableName = testTableNames.get(k);

            String type = partitiionTestType;
            for (int j = 0; j < shardKeyCols.size(); j++) {
                String col = shardKeyCols.get(j);
                String partitionContent = getPartitiionTest(type, col, funcName);

                String typeName = String.format(type, funcName);
                String realTableName = String.format("%s_%s_%s", tableName, typeName, col);
                String createSql = String.format(createSqlModel + partitionContent, realTableName);

                dropTableIfExists(realTableName);
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

                if (type.equals("dbUse%sOnly")) {
                    RuleValidator.assertMuliRuleOnlyWithDbExist(realTableName, tddlConnection);
                } else if (type.equals("tbUse%sOnly")) {
                    RuleValidator.assertMuliRuleOnlyWithTbExist(realTableName, tddlConnection);
                } else {
                    RuleValidator.assertMultiRuleWithBothDbAndTbExist(realTableName, tddlConnection);
                }

                Assert.assertTrue(getShardNum(realTableName) > 0);
                String showCreateTableString = showCreateTable(tddlConnection, realTableName);
                Assert.assertTrue(showCreateTableString.contains(partitionContent));
                dropTableIfExists(realTableName);
            }
        }

    }

}
