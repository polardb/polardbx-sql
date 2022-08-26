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

package com.alibaba.polardbx.qatest.dml.sharding.gsi;

import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.util.Pair;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Insert into gsi table with errors.
 *
 * @author minggong
 */

public class InsertGsiErrorTest extends GsiDMLTest {

    private static Map<String, String> tddlTables = new HashMap<>();
    private static Map<String, String> shadowTables = new HashMap<>();
    private static Map<String, String> mysqlTables = new HashMap<>();

    @BeforeClass
    public static void beforeCreateTables() {
        try {
            concurrentCreateNewTables(tddlTables, shadowTables, mysqlTables);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @AfterClass
    public static void afterDropTables() {

        try {
            concurrentDropTables(tddlTables, shadowTables, mysqlTables);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Parameterized.Parameters(name = "{index}:hint={0} table1={1} table2={2}")
    public static List<String[]> prepareData() {
        List<String[]> rets = Arrays.asList(new String[][] {
            {
                "", ExecuteTableName.GSI_DML_TEST + "unique_multi_index_base",
                ExecuteTableName.GSI_DML_TEST + "global_unique_one_index_base"},
            {
                HINT_STRESS_FLAG, ExecuteTableName.GSI_DML_TEST + "unique_multi_index_base",
                ExecuteTableName.GSI_DML_TEST + "global_unique_one_index_base"}
        });
        return prepareNewTableNames(rets, tddlTables, shadowTables, mysqlTables);
    }

    public InsertGsiErrorTest(String hint, String baseOneTableName, String baseTwoTableName) throws Exception {
        super(hint, baseOneTableName, baseTwoTableName);
    }

    @Before
    public void initData() throws Exception {
        super.initData();
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseTwoTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 20; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);

            params.add(param);
        }

        JdbcUtil.updateDataBatch(tddlConnection, sql, params);
        JdbcUtil.updateDataBatch(mysqlConnection, sql, params);
    }

    /**
     * insert select with uk=null
     */
    @Test
    public void insertSelectUniqueKeyNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + "(pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " select pk,null,null,null,datetime_test,null,null from " + baseTwoTableName;

//        List<Object> param = new ArrayList<>();
//
//        dataOperator
//            .executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_INSERT_UNIQUE_KEY_NULL");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert select with guk=null
     */
    @Test
    public void insertSelectGlobalUniqueKeyNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseTwoTableName
            + "(pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " select pk+100,integer_test+100,null,null,null,null,null from " + baseTwoTableName;

//        List<Object> param = new ArrayList<>();
//
//        dataOperator
//            .executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_INSERT_UNIQUE_KEY_NULL");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseTwoTableName);

        assertRouteCorrectness(baseTwoTableName);
    }

    /**
     * insert ignore select with uk=null
     */
    @Test
    public void insertIgnoreSelectUniqueKeyNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " ignore into "
            + baseOneTableName
            + "(pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " select pk,null,null,null,datetime_test,null,null from " + baseTwoTableName;

//        List<Object> param = new ArrayList<>();
//
//        dataOperator
//            .executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_INSERT_UNIQUE_KEY_NULL");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert ignore select with guk=null
     */
    @Test
    public void insertIgnoreSelectGlobalUniqueKeyNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " ignore into "
            + baseTwoTableName
            + "(pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " select pk+100,integer_test+100,null,null,null,null,null from " + baseTwoTableName;

//        List<Object> param = new ArrayList<>();
//
//        dataOperator
//            .executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_INSERT_UNIQUE_KEY_NULL");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseTwoTableName);

        assertRouteCorrectness(baseTwoTableName);
    }

    /**
     * insert select with uk=null on duplicate key update
     */
    @Test
    public void upsertSelectUniqueKeyNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + "(pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " select pk,null,null,null,datetime_test,null,null from " + baseTwoTableName
            + " on duplicate key update float_test=1";

//        List<Object> param = new ArrayList<>();
//
//        dataOperator
//            .executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_INSERT_UNIQUE_KEY_NULL");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert select with guk=null on duplicate key update
     */
    @Test
    public void upsertSelectGlobalUniqueKeyNullTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseTwoTableName
            + "(pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " select pk+100,integer_test+100,null,null,null,null,null from " + baseTwoTableName
            + " on duplicate key update float_test=1";

//        List<Object> param = new ArrayList<>();
//
//        dataOperator
//            .executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_INSERT_UNIQUE_KEY_NULL");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseTwoTableName);

        assertRouteCorrectness(baseTwoTableName);
    }

    /**
     * replace select with uk=null
     */
    @Test
    public void replaceSelectUniqueKeyNullTest() throws Exception {
        String sql = hint + "replace into " + baseOneTableName
            + "(pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " select pk,null,null,null,datetime_test,null,null from " + baseTwoTableName;

//        List<Object> param = new ArrayList<>();
//
//        dataOperator
//            .executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_INSERT_UNIQUE_KEY_NULL");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * replace select with guk=null
     */
    @Test
    public void replaceSelectGlobalUniqueKeyNullTest() throws Exception {
        String sql = hint + "replace into " + baseTwoTableName
            + "(pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " select pk+100,integer_test+100,null,null,datetime_test,null,null from " + baseTwoTableName;

//        List<Object> param = new ArrayList<>();
//
//        dataOperator
//            .executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_INSERT_UNIQUE_KEY_NULL");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseTwoTableName);

        assertRouteCorrectness(baseTwoTableName);
    }

    /**
     * insert without sharding key
     */
    @Test
    public void insertNoShardingKeyTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,varchar_test,datetime_test,year_test,char_test) values (?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
//        executeErrorAssert(tddlConnection, sql, param, "ERR_INSERT_CONTAINS_NO_SHARDING_KEY");
    }

    /**
     * insert ignore without sharding key
     */
    @Test
    public void insertIgnoreNoShardingKeyTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " ignore into "
            + baseOneTableName
            + " (pk,integer_test,varchar_test,datetime_test,year_test,char_test) values (?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
//        executeErrorAssert(tddlConnection, sql, param, "ERR_INSERT_CONTAINS_NO_SHARDING_KEY");
    }

    /**
     * insert on duplicate key update without sharding key
     */
    @Test
    public void UpsertNoShardingKeyTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?) on duplicate key update float_test=1";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
//        executeErrorAssert(tddlConnection, sql, param, "ERR_INSERT_CONTAINS_NO_SHARDING_KEY");
    }

    /**
     * replace without sharding key
     */
    @Test
    public void replaceNoShardingKeyTest() throws Exception {
        String sql = hint + "replace into " + baseOneTableName
            + " (pk,integer_test,varchar_test,datetime_test,year_test,char_test) values (?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(columnDataGenerator.year_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param);
        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
//        executeErrorAssert(tddlConnection, sql, param, "ERR_INSERT_CONTAINS_NO_SHARDING_KEY");
    }

    /**
     * insert select on duplicate key update shardingKey=xx
     */
    @Test
    public void upsertSelectUpdateShardingKeyTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName + " select * from " + baseTwoTableName
            + " on duplicate key update bigint_test=1";

//        List<Object> param = new ArrayList<>();
//
//        executeErrorAssert(tddlConnection, sql, param, "ERR_MODIFY_SHARD_COLUMN");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseTwoTableName);

        assertRouteCorrectness(baseTwoTableName);
    }

    /**
     * insert select on duplicate key update uniqueKey=xx
     */
    @Test
    public void upsertSelectUpdateUniqueKeyTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName + " select * from " + baseTwoTableName
            + " on duplicate key update varchar_test='a'";

//        List<Object> param = new ArrayList<>();
//
//        executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_MODIFY_UNIQUE_KEY");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        executeErrorAssert(mysqlConnection, sql, null, "Duplicate entry 'a' for key 'varchar_test'");
        executeErrorAssert(tddlConnection, sql, null, "Duplicate entry 'a' for key 'varchar_test'");

        sql = hint + "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseTwoTableName);

        assertRouteCorrectness(baseTwoTableName);
    }

    /**
     * insert into gsi directly
     */
    @Test
    public void insertGsiTableTest() throws Exception {
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName.replaceFirst("base", "index1")
            + " (pk,integer_test,bigint_test,varchar_test,char_test)"
            + " values (?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_DIRECTLY");
    }

    /**
     * insert into gsi directly with hint
     */
    @Test
    @Ignore("Polardbx-2.0 not support this case now")
    public void insertGsiTableHintTest() throws Exception {
        final List<Pair<String, String>> topology = JdbcUtil.getTopologyWithHint(tddlConnection,
            baseOneTableName.replaceFirst("base", "index1"), hint);
        String sql = hint + "/*+TDDL:NODE(0)*/ insert into " + topology.get(0).right
            + " (pk,integer_test,bigint_test,varchar_test,char_test)"
            + " values (?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_DIRECTLY");
    }

    /**
     * insert into primary table directly with hint
     */
    @Test
    @Ignore("Polardbx-2.0 not support this case now")
    public void insertPrimaryTableHintTest() throws Exception {
        final List<Pair<String, String>> topology =
            JdbcUtil.getTopologyWithHint(tddlConnection, baseOneTableName, hint);
        String sql = hint + "/*+TDDL:NODE(0)*/ insert into " + topology.get(0).right
            + " (pk,integer_test,bigint_test,varchar_test,char_test)"
            + " values (?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(ColumnDataGenerator.pkValue);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.bigint_testValue);
        param.add(columnDataGenerator.varchar_testValue);
        param.add(columnDataGenerator.char_testValue);

        executeErrorAssert(tddlConnection, sql, param,
            "ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_PRIMARY_TABLE_DIRECTLY");
    }

    /**
     * insert select 10001 records
     */
    @Test
    public void insertSelectExceedLimitTest() throws Exception {
        if (HINT_STRESS_FLAG.equalsIgnoreCase(hint)) {
            return;
        }

        prepareData(baseTwoTableName, 10000 + 1);

        String sql =
            "insert " + hint + "/*+TDDL:CMD_EXTRA(INSERT_SELECT_LIMIT=10000)*/ " + baseOneTableName + " select * from "
                + baseTwoTableName;

        List<Object> param = new ArrayList<>();
        //现在限制已经去掉了，所以这个语句可以执行成功
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }
}
