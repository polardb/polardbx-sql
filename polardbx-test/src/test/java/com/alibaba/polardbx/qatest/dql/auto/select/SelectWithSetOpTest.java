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

package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.base.Preconditions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * @author <a href="mailto:shitianshuo.sts@alibaba-inc.com"></a>
 * @since 5.4.14
 */
public class SelectWithSetOpTest extends BaseTestCase {

    enum SetType {
        UNIVERSE("universe", "(1), (1), (2), (2), (3), (3), (3), (4), (5), (6), (NULL), (NULL)"),
        SUB_A("sub_a", "(1), (1), (2), (2), (3) , (NULL), (NULL)"),
        SUB_A_SUB_A("sub_a_sub_a", "(1), (1) , (NULL), (NULL)"),
        SUB_A_SUB_B("sub_a_sub_b", "(2), (2) , (NULL), (NULL)"),
        SUB_A_SUB_C("sub_a_sub_c", "(3), (NULL), (NULL)"),
        SUB_B("sub_b", "(4), (4), (5), (6), (6) , (NULL), (NULL)");

        final String name;
        final String values;

        SetType(String name, String values) {
            this.name = name;
            this.values = values;
        }

        public String getName() {
            return name;
        }

        public String getValues() {
            return values;
        }
    }

    enum DataType {
        DOUBLE("double"),
        INTEGER("int");
        final String name;

        DataType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    static class MockTable {
        final SetType setType;
        final DataType dataType;
        final String tableName;

        public MockTable(SetType setType, DataType dataType) {
            this.setType = setType;
            this.dataType = dataType;
            this.tableName = genTableName(setType, dataType);
        }

        static String genTableName(SetType setType, DataType dataType) {
            return "set_op_test_" + dataType.getName() + "_" + setType.name;
        }

        String getTableName() {
            return tableName;
        }

        String getColType() {
            return dataType.getName();
        }

        String getColValues() {
            return setType.getValues();
        }

    }

    private Connection tddlConnection = getPolardbxConnection();

    void dropTableIfExists(String tableName) {
        String sql = String.format("drop table if exists %s ", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    void createTableAndGenValues(MockTable mockTable) {
        String sql = String.format("CREATE TABLE %s (x %s)", mockTable.getTableName(), mockTable.getColType());
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("insert into %s values %s", mockTable.getTableName(), mockTable.getColValues());
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    static String genSelectSql(MockTable mockTable, boolean withNull, boolean distinct) {
        return String.format("(select %s x from %s %s)",
            distinct ? "distinct" : "",
            mockTable.getTableName(),
            withNull ? "" : "where x is not null");
    }

    static String genSelectSql(MockTable mockTable, boolean withNull) {
        return genSelectSql(mockTable, withNull, false);
    }

    static String genSelectSql(MockTable mockTable) {
        return genSelectSql(mockTable, false, false);
    }

    static String unionSql(String sql1, String sql2) {
        return String.format("(%s union %s)", sql1, sql2);
    }

    List<List<Object>> executeQuery(String sql) {
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
        ResultSet tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
        return JdbcUtil.getAllResult(tddlRs);
    }

    void assertQueryResultType(String sql, int expectType) {
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
        ResultSet tddlRs = JdbcUtil.executeQuery(sql, tddlPs);
        try {
            Preconditions.checkState(tddlRs.getMetaData().getColumnType(1) == expectType, "和期望类型不一致，sql为：" + sql);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    void assertQueryEmpty(String sql) {
        Preconditions.checkState(executeQuery(sql).size() == 0, "期望为空集，sql为：" + sql);
    }

    void explainAssert(String expectPattern, String... sqls) {
        for (String sql : sqls) {
            DataValidator.explainResultMatchAssert("explain " + sql, null, tddlConnection, expectPattern);
        }
    }

    void selectContentSameAssertWithDiffSql(String standardSql, String... sqls) {
        List<List<Object>> expect = executeQuery(standardSql);
        for (String sql : sqls) {
            List<List<Object>> ret = executeQuery(sql);
            assertWithMessage(" 非顺序情况下：等价sql返回结果不同 \n sql1 语句为：" + sql + "\n stdSql 语句为: " + standardSql)
                .that(ret)
                .containsExactlyElementsIn(expect);
        }
    }

    @Before
    public void prepare() {
        for (DataType dataType : DataType.values()) {
            for (SetType setType : SetType.values()) {
                MockTable mockTable = new MockTable(setType, dataType);
                dropTableIfExists(mockTable.getTableName());
                createTableAndGenValues(mockTable);
            }
        }
    }

    @After
    public void clean() {
        for (DataType dataType : DataType.values()) {
            for (SetType setType : SetType.values()) {
                MockTable mockTable = new MockTable(setType, dataType);
                dropTableIfExists(mockTable.getTableName());
            }
        }
    }

    void intersectTypeTest(DataType universeType, DataType subType, int expectType) {
        MockTable universe = new MockTable(SetType.UNIVERSE, universeType);
        MockTable sub = new MockTable(SetType.SUB_A, subType);
        String sql = genSelectSql(universe, true) + "intersect" + genSelectSql(sub, true);
        assertQueryResultType(sql, expectType);
    }

    void intersectTest(DataType universeType, DataType subType) {
        MockTable universe = new MockTable(SetType.UNIVERSE, universeType);
        MockTable subA = new MockTable(SetType.SUB_A, subType);
        MockTable subB = new MockTable(SetType.SUB_B, subType);
        for (int i = 0; i < 2; i++, subA = subB) {
            String sql1 = genSelectSql(universe, false) + "intersect" + genSelectSql(subA, false);
            String sql2 = genSelectSql(universe, true) + "intersect" + genSelectSql(subA, false);
            String sql3 = genSelectSql(universe, false) + "intersect" + genSelectSql(subA, true);
            String stdSql = genSelectSql(subA, false, true);
            explainAssert("[\\s\\S]*SemiHashJoin[\\s\\S]*type=\"semi\"[\\s\\S]*", sql1, sql2, sql3);
            selectContentSameAssertWithDiffSql(stdSql, sql1, sql2, sql3);
        }
    }

    void intersectPartialJointTest(MockTable subA, MockTable subB, MockTable subC) {
        String sqlA = genSelectSql(subA);
        String sqlB = genSelectSql(subB);
        String sqlC = genSelectSql(subC);
        String sqlDistinctA = genSelectSql(subA, false, true);
        String sqlAB = unionSql(sqlA, sqlB);
        String sqlAC = unionSql(sqlA, sqlC);

        selectContentSameAssertWithDiffSql(sqlDistinctA, sqlAB + "intersect" + sqlAC);
    }

    void intersectPartialJointTest(DataType dataType) {
        MockTable subA = new MockTable(SetType.SUB_A_SUB_A, dataType);
        MockTable subB = new MockTable(SetType.SUB_A_SUB_B, dataType);
        MockTable subC = new MockTable(SetType.SUB_A_SUB_C, dataType);
        intersectPartialJointTest(subA, subB, subC);
        intersectPartialJointTest(subB, subA, subC);
        intersectPartialJointTest(subC, subB, subA);
    }

    void intersectDisjointTest(MockTable subA, MockTable subB) {
        String sqlA = genSelectSql(subA);
        String sqlB = genSelectSql(subB);

        assertQueryEmpty(sqlA + "intersect" + sqlB);
    }

    void intersectDisjointTest(DataType dataType) {
        MockTable subA = new MockTable(SetType.SUB_A_SUB_A, dataType);
        MockTable subB = new MockTable(SetType.SUB_A_SUB_B, dataType);
        MockTable subC = new MockTable(SetType.SUB_A_SUB_C, dataType);
        intersectDisjointTest(subA, subB);
        intersectDisjointTest(subB, subC);
        intersectDisjointTest(subC, subA);
    }

    void intersectEqualTest(DataType dataType) {
        MockTable set = new MockTable(SetType.UNIVERSE, dataType);
        String sql = genSelectSql(set, true, false);
        String sqlDistinct = genSelectSql(set, true, true);

        selectContentSameAssertWithDiffSql(sqlDistinct, sql + "intersect" + sql);
    }

    void intersectWithNullTest(DataType universeType, DataType subType) {
        MockTable universe = new MockTable(SetType.UNIVERSE, universeType);
        MockTable subA = new MockTable(SetType.SUB_A, subType);
        MockTable subB = new MockTable(SetType.SUB_B, subType);
        for (int i = 0; i < 2; i++, subA = subB) {
            String sql = genSelectSql(universe, true) + "intersect" + genSelectSql(subA, true);
            String stdSql = genSelectSql(subA, true, true);
            explainAssert("[\\s\\S]*SemiHashJoin[\\s\\S]*type=\"semi\"[\\s\\S]*", sql);
            selectContentSameAssertWithDiffSql(stdSql, sql);
        }
    }

    void minusTypeTest(DataType universeType, DataType subType, int expectType) {
        MockTable universe = new MockTable(SetType.UNIVERSE, universeType);
        MockTable sub = new MockTable(SetType.SUB_A, subType);
        for (String setOp : new String[] {"minus", "except"}) {
            String sql = genSelectSql(universe, true) + setOp + genSelectSql(sub, true);
            assertQueryResultType(sql, expectType);
        }
    }

    void minusPartialJointTest(MockTable subA, MockTable subB, MockTable subC) {
        String sqlA = genSelectSql(subA);
        String sqlB = genSelectSql(subB);
        String sqlC = genSelectSql(subC);
        String sqlAB = unionSql(sqlA, sqlB);
        String sqlAC = unionSql(sqlA, sqlC);
        String sqlDistinctB = genSelectSql(subB, false, true);
        selectContentSameAssertWithDiffSql(sqlDistinctB, sqlAB + "minus" + sqlAC, sqlAB + "except" + sqlAC);
    }

    void minusPartialJointTest(DataType dataType) {
        MockTable subA = new MockTable(SetType.SUB_A_SUB_A, dataType);
        MockTable subB = new MockTable(SetType.SUB_A_SUB_B, dataType);
        MockTable subC = new MockTable(SetType.SUB_A_SUB_C, dataType);
        minusPartialJointTest(subA, subB, subC);
        minusPartialJointTest(subB, subA, subC);
        minusPartialJointTest(subC, subB, subA);
    }

    void minusDisjointTest(MockTable subA, MockTable subB) {
        String sqlA = genSelectSql(subA);
        String sqlB = genSelectSql(subB);
        String sqlDistinctA = genSelectSql(subA, false, true);
        selectContentSameAssertWithDiffSql(sqlDistinctA, sqlA + "minus" + sqlB, sqlA + "except" + sqlB);
    }

    void minusDisjointTest(DataType dataType) {
        MockTable subA = new MockTable(SetType.SUB_A_SUB_A, dataType);
        MockTable subB = new MockTable(SetType.SUB_A_SUB_B, dataType);
        MockTable subC = new MockTable(SetType.SUB_A_SUB_C, dataType);
        minusDisjointTest(subA, subB);
        minusDisjointTest(subB, subC);
        minusDisjointTest(subC, subA);
    }

    void minusEqualTest(DataType dataType) {
        MockTable set = new MockTable(SetType.UNIVERSE, dataType);
        String sql = genSelectSql(set, true, false);
        assertQueryEmpty(sql + "minus" + sql);
    }

    void minusTest(DataType universeType, DataType subType) {
        MockTable universe = new MockTable(SetType.UNIVERSE, universeType);
        MockTable subA = new MockTable(SetType.SUB_A, subType);
        MockTable subB = new MockTable(SetType.SUB_B, subType);
        MockTable tmp;
        for (int i = 0; i < 2; i++, tmp = subA, subA = subB, subB = tmp) {
            for (String setOp : new String[] {"minus", "except"}) {
                String stdSql = genSelectSql(subB, false, true);
                String sql1 = genSelectSql(universe, false) + setOp + genSelectSql(subA, false);
                String sql2 = genSelectSql(universe, true) + setOp + genSelectSql(subA, true);
                String sql3 = genSelectSql(universe, false) + setOp + genSelectSql(subA, true);
                explainAssert("[\\s\\S]*SemiHashJoin[\\s\\S]*type=\"anti\"[\\s\\S]*", sql1, sql2, sql3);
                selectContentSameAssertWithDiffSql(stdSql, sql1, sql2, sql3);
            }
        }
    }

    void minusWithNullTest(DataType universeType, DataType subType) {
        MockTable universe = new MockTable(SetType.UNIVERSE, universeType);
        MockTable subA = new MockTable(SetType.SUB_A, subType);
        MockTable subB = new MockTable(SetType.SUB_B, subType);
        MockTable tmp;
        for (int i = 0; i < 2; i++, tmp = subA, subA = subB, subB = tmp) {
            for (String setOp : new String[] {"minus", "except"}) {
                String stdSql = genSelectSql(subB, true, true);
                String sql = genSelectSql(universe, true) + setOp + genSelectSql(subA, false);
                explainAssert("[\\s\\S]*SemiHashJoin[\\s\\S]*type=\"anti\"[\\s\\S]*", sql);
                selectContentSameAssertWithDiffSql(stdSql, sql);
            }
        }

    }

    /**
     * @since 5.4.13
     */
    @Test
    public void intersectTest() {
        for (DataType dataType : DataType.values()) {
            intersectTest(dataType, dataType);
            intersectPartialJointTest(dataType);
            intersectDisjointTest(dataType);
            intersectEqualTest(dataType);
        }
    }

    /**
     * @since 5.4.13
     */
    @Test
    public void intersectWithNullTest() {
        for (DataType dataType : DataType.values()) {
            intersectWithNullTest(dataType, dataType);
        }
    }

    /**
     * @since 5.4.13
     */
    @Test
    public void minusTest() {
        for (DataType dataType : DataType.values()) {
            minusTest(dataType, dataType);
            minusPartialJointTest(dataType);
            minusDisjointTest(dataType);
            minusEqualTest(dataType);
        }
    }

    /**
     * @since 5.4.13
     */
    @Test
    public void minusWithNullTest() {
        for (DataType dataType : DataType.values()) {
            minusWithNullTest(dataType, dataType);
        }
    }

    /**
     * @since 5.4.13
     */
    @Test
    public void intersectWithConvertTest() {
        intersectTest(DataType.DOUBLE, DataType.INTEGER);
        intersectTest(DataType.INTEGER, DataType.DOUBLE);
    }

    /**
     * @since 5.4.13
     */
    @Test
    public void intersectWithNullWithConvertTest() {
        intersectWithNullTest(DataType.DOUBLE, DataType.INTEGER);
        intersectWithNullTest(DataType.INTEGER, DataType.DOUBLE);

    }

    /**
     * @since 5.4.13
     */
    @Test
    public void minusWithConvertTest() {
        minusTest(DataType.DOUBLE, DataType.INTEGER);
        minusTest(DataType.INTEGER, DataType.DOUBLE);
    }

    /**
     * @since 5.4.13
     */
    @Test
    public void minusWithNullWithConvertTest() {
        minusWithNullTest(DataType.DOUBLE, DataType.INTEGER);
        minusWithNullTest(DataType.INTEGER, DataType.DOUBLE);
    }

    /**
     * @since 5.4.13
     */
    @Test
    public void intersectTypeConvertTest() {
        intersectTypeTest(DataType.DOUBLE, DataType.INTEGER, Types.DOUBLE);
        intersectTypeTest(DataType.INTEGER, DataType.DOUBLE, Types.DOUBLE);
    }

    /**
     * @since 5.4.13
     */
    @Test
    public void minusTypeConvertTest() {
        minusTypeTest(DataType.DOUBLE, DataType.INTEGER, Types.DOUBLE);
        minusTypeTest(DataType.INTEGER, DataType.DOUBLE, Types.DOUBLE);
    }

    static MockTable UNIVERSE = new MockTable(SetType.SUB_A, DataType.INTEGER);
    static MockTable SUB_A = new MockTable(SetType.SUB_A_SUB_A, DataType.INTEGER);
    static MockTable SUB_B = new MockTable(SetType.SUB_A_SUB_B, DataType.INTEGER);
    static MockTable SUB_C = new MockTable(SetType.SUB_A_SUB_C, DataType.INTEGER);
    static String SQL_ABC = genSelectSql(UNIVERSE, false, true);
    static String SQL_A = genSelectSql(SUB_A, false, true);
    static String SQL_B = genSelectSql(SUB_B, false, true);
    static String SQL_C = genSelectSql(SUB_C, false, true);
    static String SQL_AB = unionSql(SQL_A, SQL_B);
    static String SQL_BC = unionSql(SQL_B, SQL_C);
    static String SQL_CA = unionSql(SQL_C, SQL_A);

    /**
     * @since 5.4.13
     */
    @Test
    public void intersectUnionTest() {
        String stdSql = SQL_BC;
        String sql1 = SQL_AB + "intersect" + SQL_BC + "union" + SQL_C;
        String sql2 = SQL_ABC + "intersect" + SQL_B + "union" + SQL_C;
        String sql3 = SQL_CA + "intersect" + SQL_BC + "union" + SQL_B;

        selectContentSameAssertWithDiffSql(stdSql, sql1, sql2, sql3);
    }

    /**
     * @since 5.4.13
     */
    @Test
    public void intersectMinusTest() {
        String stdSql = SQL_A;
        String sql1 = SQL_AB + "intersect" + SQL_CA + "minus" + SQL_B;
        String sql2 = SQL_ABC + "intersect" + SQL_CA + "minus" + SQL_C;
        String sql3 = SQL_CA + "intersect" + SQL_ABC + "minus" + SQL_C;

        selectContentSameAssertWithDiffSql(stdSql, sql1, sql2, sql3);
    }

    /**
     * @since 5.4.13
     */
    @Test
    public void unionMinusTest() {
        String stdSql = SQL_A;
        String sql1 = SQL_AB + "union" + SQL_CA + "minus" + SQL_BC;
        String sql2 = SQL_AB + "union" + SQL_CA + "minus" + SQL_B + "minus" + SQL_C;
        String sql3 = SQL_B + "union" + SQL_A + "minus" + SQL_B;

        selectContentSameAssertWithDiffSql(stdSql, sql1, sql2, sql3);
    }

    /**
     * @since 5.4.13
     */
    @Test
    public void minusUnionTest() {
        String stdSql = SQL_AB;
        String sql1 = SQL_AB + "minus" + SQL_B + "union" + SQL_B;
        String sql2 = SQL_ABC + "minus" + SQL_CA + "union" + SQL_A;
        String sql3 = SQL_BC + "minus" + SQL_BC + "union" + SQL_AB;

        selectContentSameAssertWithDiffSql(stdSql, sql1, sql2, sql3);

    }

    /**
     * @since 5.4.13
     * in current version, Intersect has same precedence with other setOp
     * different wit SQL-03
     */
    @Test
    public void unionIntersectTest() {
        String stdSql = SQL_A;
        String sql1 = SQL_AB + "union" + SQL_CA + "intersect" + SQL_A;
        String sql2 = SQL_A + "union" + SQL_B + "intersect" + SQL_CA;
        String sql3 = SQL_BC + "union" + SQL_A + "intersect" + SQL_A;

        selectContentSameAssertWithDiffSql(stdSql, sql1, sql2, sql3);
    }

    /**
     * @since 5.4.13
     * in current version, Intersect has same precedence with other setOp
     * different wit SQL-03
     */
    @Test
    public void minusIntersectTest() {
        String stdSql = SQL_A;
        String sql1 = SQL_ABC + "minus" + SQL_C + "intersect" + SQL_A;
        String sql2 = SQL_ABC + "minus" + SQL_C + "intersect" + SQL_CA;
        String sql3 = SQL_ABC + "minus" + SQL_BC + "intersect" + SQL_A;

        selectContentSameAssertWithDiffSql(stdSql, sql1, sql2, sql3);

    }

}


