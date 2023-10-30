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

import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.util.Pair;
import org.junit.AfterClass;
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


public class DeleteGsiErrorTest extends GsiDMLTest {

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
                ExecuteTableName.GSI_DML_TEST + "unique_one_index_base"},
            {
                HINT_STRESS_FLAG, ExecuteTableName.GSI_DML_TEST + "unique_multi_index_base",
                ExecuteTableName.GSI_DML_TEST + "unique_one_index_base"}
        });
        return prepareNewTableNames(rets, tddlTables, shadowTables, mysqlTables);
    }

    public DeleteGsiErrorTest(String hint, String baseOneTableName, String baseTwoTableName) throws Exception {
        super(hint, baseOneTableName, baseTwoTableName);
    }

    /**
     * delete from gsi directly
     */
    @Test
    public void deleteGsiTableTest() throws Exception {
        String sql = String.format(hint + "delete from %s", baseOneTableName.replaceFirst("base", "index1"));

        List<Object> param = new ArrayList<>();

        executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_DIRECTLY");
    }

    /**
     * delete from gsi directly with hint
     */
    @Test
    @Ignore("Polardbx-2.0 not support this case now")
    public void deleteGsiTableHintTest() throws Exception {
        final List<Pair<String, String>> topology = JdbcUtil.getTopologyWithHint(tddlConnection,
            baseOneTableName.replaceFirst("base", "index1"), hint);
        String sql = hint + "/*+TDDL:NODE(0)*/ delete from " + topology.get(0).right;

        List<Object> param = new ArrayList<>();

        executeErrorAssert(tddlConnection, sql, param, "ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_DIRECTLY");
    }

    /**
     * delete from primary table directly with hint
     */
    @Test
    @Ignore("Polardbx-2.0 not support this case now")
    public void deletePrimaryTableHintTest() throws Exception {
        final List<Pair<String, String>> topology =
            JdbcUtil.getTopologyWithHint(tddlConnection, baseOneTableName, hint);
        String sql = hint + "/*+TDDL:NODE(0)*/ delete from " + topology.get(0).right;

        List<Object> param = new ArrayList<>();

        executeErrorAssert(tddlConnection, sql, param,
            "ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_PRIMARY_TABLE_DIRECTLY");
    }
}
