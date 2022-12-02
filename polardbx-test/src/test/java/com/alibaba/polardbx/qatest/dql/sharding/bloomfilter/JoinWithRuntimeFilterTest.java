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

package com.alibaba.polardbx.qatest.dql.sharding.bloomfilter;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 验证CN与DN bloomfilter实现的一致性
 * 包括hash算法、probe算法与类型支持
 */
@RunWith(Parameterized.class)
public class JoinWithRuntimeFilterTest extends ReadBaseTestCase {

    private String baseOneTableName;
    private String baseTwoTableName;
    private String hint;

    private static final String MURMUR3_HINT =
        "/*+TDDL:ENABLE_RUNTIME_FILTER_XXHASH=false ENABLE_PUSH_RUNTIME_FILTER_SCAN=true ENABLE_RUNTIME_FILTER=true "
            + " ENABLE_BKA_JOIN=false RUNTIME_FILTER_PROBE_MIN_ROW_COUNT=-1 "
            + " FORCE_ENABLE_RUNTIME_FILTER_COLUMNS=(\"integer_test,pk\")*/ ";

    private static final String XXHASH_HINT =
        "/*+TDDL:ENABLE_RUNTIME_FILTER_XXHASH=true ENABLE_PUSH_RUNTIME_FILTER_SCAN=true ENABLE_RUNTIME_FILTER=true "
            + " ENABLE_BKA_JOIN=false RUNTIME_FILTER_PROBE_MIN_ROW_COUNT=-1 "
            + " FORCE_ENABLE_RUNTIME_FILTER_COLUMNS=(\"integer_test,pk\")*/ ";

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1},hint={2}")
    public static List<String[]> prepareDate() throws SQLException {
        boolean supportXxHash = false;
        try (Connection conn = ConnectionManager.getInstance().newMysqlConnection();
            Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("show variables like 'udf_bloomfilter_xxhash'");
            supportXxHash = rs.next();
        }
        String[][] tablesList = ExecuteTableSelect.selectBaseOneBaseTwo();
        List<String[]> ret = new ArrayList<>(tablesList.length);
        for (String[] tables : tablesList) {
            String[] row = new String[tables.length + 1];
            System.arraycopy(tables, 0, row, 0, tables.length);
            row[tables.length] = MURMUR3_HINT;
            ret.add(row);
            if (supportXxHash) {
                row = new String[tables.length + 1];
                System.arraycopy(tables, 0, row, 0, tables.length);
                row[tables.length] = XXHASH_HINT;
                ret.add(row);
            }
        }
        return ret;
    }

    public JoinWithRuntimeFilterTest(String baseOneTableName, String baseTwoTableName, String hint) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
        this.hint = hint;
    }

    @Test
    public void joinWithRuntimeFilterTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test from " + baseOneTableName + " inner join "
                + baseTwoTableName
                + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
