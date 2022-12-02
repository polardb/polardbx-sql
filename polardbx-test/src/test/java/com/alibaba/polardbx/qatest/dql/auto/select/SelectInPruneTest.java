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
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Set;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author fangwu
 * prune args inside of in expr
 */
public class SelectInPruneTest extends BaseTestCase {

    public static final String MULTI_TABLE_MULTI_PARTITION_COLS_NAME =
        "__PRUNE_IN_TEST_TABLE_MULTI_TABLE_MULTI_PARTITION_COLS__";
    public static final String MULTI_TABLE_MULTI_PARTITION_COLS_CREATE =
        "\n" + "CREATE TABLE IF NOT EXISTS `" + MULTI_TABLE_MULTI_PARTITION_COLS_NAME + "` (\n"
            + "\t`pk` bigint(11) NOT NULL,\n"
            + "\t`integer_test` int(11) DEFAULT NULL,\n" + "\t`varchar_test` varchar(255) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`pk`) tbpartition by hash(`varchar_test`) tbpartitions 4";
    public static final String multiPartitionOneColumnSql =
        "SELECT * FROM " + MULTI_TABLE_MULTI_PARTITION_COLS_NAME + " WHERE pk IN (1, 6, 9, 23, 11, 23, 894)";
    public static final String multiPartitionMultiColumnSql =
        "SELECT * FROM " + MULTI_TABLE_MULTI_PARTITION_COLS_NAME
            + " WHERE (pk,varchar_test) IN ((1, 'word23'), (6, 'aa'), (9,'0'), (23,'33'), (11, '12'), (23, '894'))";
    public static final String multiPartitionMultiColumnWithDisturbExprSql =
        "SELECT * FROM " + MULTI_TABLE_MULTI_PARTITION_COLS_NAME
            + " WHERE (pk,varchar_test) "
            + "IN ((1, '2'), (6, '3'), (9,'0'), (23,'33'), (11, '12'), (23, '894')) OR (pk>100 AND pk<102)";
    public static final String multiPartitionmultiColumnWithDisturbExprAndOrderSql =
        "SELECT * FROM " + MULTI_TABLE_MULTI_PARTITION_COLS_NAME
            + " WHERE (pk,varchar_test) IN ((101, 'abdfeed'), (6, '3'), (9,'0'), (23,'33'), (106, 'cdefeed'), (23, '894')) OR (pk>100 AND pk<102) order by integer_test limit 0, 100";

    public static final String multiPartitionMultiColumnReverseSql =
        "SELECT * FROM " + MULTI_TABLE_MULTI_PARTITION_COLS_NAME
            + " WHERE (integer_test, varchar_test, pk) IN ((59, 'einoejk', 106), (6, 'er', 3), (9, 'ab', 0), (23, 'e',33), (11, 'fj', 12), (23, 'jf', 894))";

    public static final String MULTI_TABLE_ONE_PARTITION_COL_NAME = "__PRUNE_IN_TEST_MULTI_TABLE_ONE_PARTITION_COL__";
    public static final String MULTI_TABLE_ONE_PARTITION_COL_CREATE =
        "\n" + "CREATE TABLE IF NOT EXISTS `" + MULTI_TABLE_ONE_PARTITION_COL_NAME + "` (\n"
            + "\t`pk` bigint(11) NOT NULL,\n"
            + "\t`integer_test` int(11) DEFAULT NULL,\n" + "\t`varchar_test` varchar(255) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`pk`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`pk`) tbpartition by hash(`pk`) tbpartitions 4";
    public static final String oneColumnSql =
        "SELECT * FROM " + MULTI_TABLE_ONE_PARTITION_COL_NAME + " WHERE pk IN (1, 6, 9, 23, 11, 23, 894)";

    public static final String multiColumnSql =
        "SELECT * FROM " + MULTI_TABLE_ONE_PARTITION_COL_NAME
            + " WHERE (pk,integer_test) IN ((1, 18), (6, 3), (9,0), (23,33), (11, 12), (23, 894))";

    public static final String multiColumnWithDisturbExprSql =
        "SELECT * FROM " + MULTI_TABLE_ONE_PARTITION_COL_NAME + " WHERE (pk,integer_test) "
            + "IN ((1, 18), (6, 3), (9,0), (23,33), (11, 12), (23, 894)) OR (pk>100 AND pk<102)";

    public static final String multiColumnWithDisturbExprAndOrderSql =
        "SELECT * FROM " + MULTI_TABLE_ONE_PARTITION_COL_NAME + " WHERE (pk,integer_test) "
            + "IN ((1, 2), (6, 3), (9,0), (23,33), (11, 12), (23, 894)) OR (pk>100 AND pk<103) order by integer_test limit 1, 100";

    public static final String multiColumnReversSql =
        "SELECT * FROM " + MULTI_TABLE_ONE_PARTITION_COL_NAME
            + " WHERE (integer_test, pk) IN ((18, 1), (6, 3), (9,0), (23,33), (11, 12), (23, 894))";

    public static final String multiColumnReversMultiTableSql =
        "SELECT * FROM " + MULTI_TABLE_ONE_PARTITION_COL_NAME
            + " a JOIN " + MULTI_TABLE_MULTI_PARTITION_COLS_NAME
            + " b ON a.pk = b.integer_test WHERE (b.integer_test) IN (18,6, 3, 9,0, 23,33, 11, 12, 23, 894)";

    public static final String pruneRawRegex = "PruneRaw(\\s\\S+)";
    public static final String hint = " /*TDDL:IN_PRUNE_SIZE=1 IN_PRUNE_STEP_SIZE=1*/ ";

    /**
     * multi partition column and single in expr
     */
    @Test
    public void selectInMultiPartitionMultiColumn() throws SQLException {
        Statement stmt = this.getPolardbxConnection().createStatement();
        stmt.execute(MULTI_TABLE_MULTI_PARTITION_COLS_CREATE);

        stmt.executeQuery("trace /*TDDL:IN_PRUNE_SIZE=1 IN_PRUNE_STEP_SIZE=1*/ " + multiPartitionMultiColumnSql);
        ResultSet resultSet = stmt.executeQuery("show trace");
        boolean hasPrune = false;
        while (resultSet.next()) {
            String row = resultSet.getString("PARAMS");
            if (hasPruneString(row)) {
                hasPrune = true;
                break;
            }
        }

        // sets empty meaning xprotocol or pruning was not being actived.
        if (!hasPrune) {
            return;
        }

        // check result correctness
        String checkSql = hint + multiPartitionMultiColumnSql.replace(MULTI_TABLE_MULTI_PARTITION_COLS_NAME,
            "select_base_one_multi_db_multi_tb");
        selectContentSameAssert(checkSql, null, this.getMysqlConnection(),
            this.getPolardbxConnection());
    }

    /**
     * multi partition column and single in expr
     */
    @Test
    public void selectInMultiPartitionOneColumn() throws SQLException {
        Statement stmt = this.getPolardbxConnection().createStatement();
        stmt.execute(MULTI_TABLE_MULTI_PARTITION_COLS_CREATE);

        stmt.executeQuery("trace /*TDDL:IN_PRUNE_SIZE=1 IN_PRUNE_STEP_SIZE=1*/ " + multiPartitionOneColumnSql);
        ResultSet resultSet = stmt.executeQuery("show trace");
        boolean hasPrune = false;
        while (resultSet.next()) {
            String row = resultSet.getString("PARAMS");
            if (hasPruneString(row)) {
                hasPrune = true;
                break;
            }
        }

        // sets empty meaning xprotocol or pruning was not being actived.
        if (!hasPrune) {
            return;
        }
        // check result correctness
        String checkSql = hint + multiPartitionOneColumnSql.replace(MULTI_TABLE_MULTI_PARTITION_COLS_NAME,
            "select_base_one_multi_db_multi_tb");
        selectContentSameAssert(checkSql, null, this.getMysqlConnection(),
            this.getPolardbxConnection());
    }

    /**
     * multi partition column and multi column in expr with disturb expr
     */
    @Test
    public void selectInMultiPartitionMultiColumnWithDisburbExpr() throws SQLException {
        Statement stmt = this.getPolardbxConnection().createStatement();
        stmt.execute(MULTI_TABLE_MULTI_PARTITION_COLS_CREATE);

        stmt.executeQuery(
            "trace /*TDDL:IN_PRUNE_SIZE=1 IN_PRUNE_STEP_SIZE=1*/ " + multiPartitionMultiColumnWithDisturbExprSql);
        ResultSet resultSet = stmt.executeQuery("show trace");
        boolean hasPrune = false;
        while (resultSet.next()) {
            String row = resultSet.getString("PARAMS");
            if (hasPruneString(row)) {
                hasPrune = true;
                break;
            }
        }

        // sets empty meaning xprotocol or pruning was not being actived.
        if (!hasPrune) {
            return;
        }
        // check result correctness
        String checkSql =
            hint + multiPartitionMultiColumnWithDisturbExprSql.replace(MULTI_TABLE_MULTI_PARTITION_COLS_NAME,
                "select_base_one_multi_db_multi_tb");
        selectContentSameAssert(checkSql, null, this.getMysqlConnection(),
            this.getPolardbxConnection());
    }

    /**
     * multi partition column and multi column in expr with disturb expr and order
     */
    @Test
    public void selectInMultiPartitionMultiColumnWithDisburbExprAndOrder() throws SQLException {
        Statement stmt = this.getPolardbxConnection().createStatement();
        stmt.execute(MULTI_TABLE_MULTI_PARTITION_COLS_CREATE);

        stmt.executeQuery("trace /*TDDL:IN_PRUNE_SIZE=1 IN_PRUNE_STEP_SIZE=1*/ "
            + multiPartitionmultiColumnWithDisturbExprAndOrderSql);
        ResultSet resultSet = stmt.executeQuery("show trace");
        boolean hasPrune = false;
        while (resultSet.next()) {
            String row = resultSet.getString("PARAMS");
            if (hasPruneString(row)) {
                hasPrune = true;
                break;
            }
        }

        // sets empty meaning xprotocol or pruning was not being actived.
        if (!hasPrune) {
            return;
        }

        // check result correctness
        String checkSql =
            hint + multiPartitionmultiColumnWithDisturbExprAndOrderSql.replace(MULTI_TABLE_MULTI_PARTITION_COLS_NAME,
                "select_base_one_multi_db_multi_tb");
        selectContentSameAssert(checkSql, null, this.getMysqlConnection(),
            this.getPolardbxConnection());
    }

    /**
     * multi partition column and single in expr
     */
    @Test
    public void selectInMultiPartitionMultiColumnReverse() throws SQLException {
        Statement stmt = this.getPolardbxConnection().createStatement();
        stmt.execute(MULTI_TABLE_MULTI_PARTITION_COLS_CREATE);

        stmt.executeQuery("trace " + hint + multiColumnReversMultiTableSql);
        ResultSet resultSet = stmt.executeQuery("show trace");
        boolean hasPrune = false;
        while (resultSet.next()) {
            String row = resultSet.getString("PARAMS");
            if (hasPruneString(row)) {
                hasPrune = true;
                break;
            }
        }

        // sets empty meaning xprotocol or pruning was not being actived.
        if (!hasPrune) {
            return;
        }
        // check result correctness
        String checkSql = hint + multiPartitionMultiColumnReverseSql.replace(MULTI_TABLE_ONE_PARTITION_COL_NAME,
                "select_base_one_multi_db_multi_tb")
            .replace(MULTI_TABLE_MULTI_PARTITION_COLS_NAME, "select_base_one_multi_db_multi_tb");
        selectContentSameAssert(checkSql, null, this.getMysqlConnection(),
            this.getPolardbxConnection());
    }

    /**
     * multi partition column and single in expr
     */
    @Test
    public void selectInMultiPartitionMultiTableMultiColumnReverse() throws SQLException {
        Statement stmt = this.getPolardbxConnection().createStatement();
        stmt.execute(MULTI_TABLE_MULTI_PARTITION_COLS_CREATE);

        stmt.executeQuery("trace /*TDDL:IN_PRUNE_SIZE=1 IN_PRUNE_STEP_SIZE=1*/ " + multiPartitionMultiColumnReverseSql);
        ResultSet resultSet = stmt.executeQuery("show trace");
        boolean hasPrune = false;
        while (resultSet.next()) {
            String row = resultSet.getString("PARAMS");
            if (hasPruneString(row)) {
                hasPrune = true;
                break;
            }
        }

        // sets empty meaning xprotocol or pruning was not being actived.
        if (!hasPrune) {
            return;
        }

        // check result correctness
        String checkSql = hint + multiPartitionMultiColumnReverseSql.replace(MULTI_TABLE_MULTI_PARTITION_COLS_NAME,
            "select_base_one_multi_db_multi_tb");
        selectContentSameAssert(checkSql, null, this.getMysqlConnection(),
            this.getPolardbxConnection());
    }

    /**
     * single partition column and single in expr
     */
    @Test
    public void selectInOneColumn() throws SQLException {
        Statement stmt = this.getPolardbxConnection().createStatement();
        stmt.execute(MULTI_TABLE_ONE_PARTITION_COL_CREATE);

        stmt.executeQuery("trace /*TDDL:IN_PRUNE_SIZE=1 IN_PRUNE_STEP_SIZE=1*/ " + oneColumnSql);
        ResultSet resultSet = stmt.executeQuery("show trace");
        boolean hasPrune = false;
        while (resultSet.next()) {
            String row = resultSet.getString("PARAMS");
            if (hasPruneString(row)) {
                hasPrune = true;
                break;
            }
        }

        // sets empty meaning xprotocol or pruning was not being actived.
        if (!hasPrune) {
            return;
        }

        // check result correctness
        String checkSql = hint + oneColumnSql.replace(MULTI_TABLE_ONE_PARTITION_COL_NAME,
            "select_base_one_multi_db_multi_tb");
        selectContentSameAssert(checkSql, null, this.getMysqlConnection(),
            this.getPolardbxConnection());
    }

    /**
     * single partition column and multi in expr
     */
    @Test
    public void selectInMultiColumn() throws SQLException {
        Statement stmt = this.getPolardbxConnection().createStatement();
        stmt.execute(MULTI_TABLE_ONE_PARTITION_COL_CREATE);

        stmt.executeQuery("trace /*TDDL:IN_PRUNE_SIZE=1 IN_PRUNE_STEP_SIZE=1*/ " + multiColumnSql);
        ResultSet resultSet = stmt.executeQuery("show trace");
        boolean hasPrune = false;
        while (resultSet.next()) {
            String row = resultSet.getString("PARAMS");
            if (hasPruneString(row)) {
                hasPrune = true;
                break;
            }
        }

        // sets empty meaning xprotocol or pruning was not being actived.
        if (!hasPrune) {
            return;
        }

        // check result correctness
        String checkSql = hint + multiColumnSql.replace(MULTI_TABLE_ONE_PARTITION_COL_NAME,
            "select_base_one_multi_db_multi_tb");
        selectContentSameAssert(checkSql, null, this.getMysqlConnection(),
            this.getPolardbxConnection());
    }

    /**
     * single partition column and multi in expr with disturb expr
     */
    @Test
    public void selectInMultiColumnWithDisturbExpr() throws SQLException {
        Statement stmt = this.getPolardbxConnection().createStatement();
        stmt.execute(MULTI_TABLE_ONE_PARTITION_COL_CREATE);

        stmt.executeQuery("trace /*TDDL:IN_PRUNE_SIZE=1 IN_PRUNE_STEP_SIZE=1*/ " + multiColumnWithDisturbExprSql);
        ResultSet resultSet = stmt.executeQuery("show trace");
        boolean hasPrune = false;
        while (resultSet.next()) {
            String row = resultSet.getString("PARAMS");
            if (hasPruneString(row)) {
                hasPrune = true;
                break;
            }
        }

        // sets empty meaning xprotocol or pruning was not being actived.
        if (!hasPrune) {
            return;
        }

        // check result correctness
        String checkSql = hint + multiColumnWithDisturbExprSql.replace(MULTI_TABLE_ONE_PARTITION_COL_NAME,
            "select_base_one_multi_db_multi_tb");
        selectContentSameAssert(checkSql, null, this.getMysqlConnection(),
            this.getPolardbxConnection());
    }

    /**
     * single partition column and multi in expr with disturb expr and order by
     */
    @Test
    public void selectInMultiColumnWithDisturbExprAndOrder() throws SQLException {
        Statement stmt = this.getPolardbxConnection().createStatement();
        stmt.execute(MULTI_TABLE_ONE_PARTITION_COL_CREATE);

        stmt.executeQuery(
            "trace /*TDDL:IN_PRUNE_SIZE=1 IN_PRUNE_STEP_SIZE=1*/ " + multiColumnWithDisturbExprAndOrderSql);
        ResultSet resultSet = stmt.executeQuery("show trace");
        boolean hasPrune = false;
        while (resultSet.next()) {
            String row = resultSet.getString("PARAMS");
            if (hasPruneString(row)) {
                hasPrune = true;
                break;
            }
        }

        // sets empty meaning xprotocol or pruning was not being actived.
        if (!hasPrune) {
            return;
        }

        // check result correctness
        String checkSql = hint + multiColumnWithDisturbExprAndOrderSql.replace(MULTI_TABLE_ONE_PARTITION_COL_NAME,
            "select_base_one_multi_db_multi_tb");
        selectContentSameAssert(checkSql, null, this.getMysqlConnection(),
            this.getPolardbxConnection());
    }

    /**
     * single partition column and multi in expr, reverse args side
     */
    @Test
    public void selectInMultiColumnReverse() throws SQLException {
        Statement stmt = this.getPolardbxConnection().createStatement();
        stmt.execute(MULTI_TABLE_ONE_PARTITION_COL_CREATE);

        stmt.executeQuery("trace /*TDDL:IN_PRUNE_SIZE=1 IN_PRUNE_STEP_SIZE=1*/ " + multiColumnReversSql);
        ResultSet resultSet = stmt.executeQuery("show trace");
        boolean hasPrune = false;
        while (resultSet.next()) {
            String row = resultSet.getString("PARAMS");
            if (hasPruneString(row)) {
                hasPrune = true;
                break;
            }
        }

        // sets empty meaning xprotocol or pruning was not being actived.
        if (!hasPrune) {
            return;
        }

        // check result correctness
        String checkSql = hint + multiColumnReversSql.replace(MULTI_TABLE_ONE_PARTITION_COL_NAME,
            "select_base_one_multi_db_multi_tb");
        selectContentSameAssert(checkSql, null, this.getMysqlConnection(),
            this.getPolardbxConnection());
    }

    private boolean equalsCheck(Set<String> checkSets, StringBuilder pruneString) {
        String checkString = checkSets.toString();
        checkString = buildSortedString(checkString);
        String prString = buildSortedString(pruneString.toString());
        return checkString.equals(prString);
    }

    private String buildSortedString(String checkString) {
        String rs = checkString.replaceAll("[\\s \\[\\](),]", "");
        char[] c = rs.toCharArray();
        Arrays.sort(c);
        return new String(c);
    }

    private boolean hasPruneString(String row) {
        return row.indexOf("PruneRaw(") != -1;
    }

    public static final String[][] dynamicInSqlTest = {
        {"explain select * from select_base_one_multi_db_multi_tb where pk in (1,2,3,4, 5)", "true"},
        {"explain select * from select_base_one_multi_db_multi_tb where varchar_test in ('1','4',' 5')", "false"},
        {"explain select * from select_base_one_multi_db_multi_tb where pk in (1,2,3,4, 5+7)", "false"},
        {"explain select * from select_base_one_multi_db_multi_tb where pk in (1,2,3,4, integer_test)", "false"},
        {"explain select * from select_base_one_multi_db_multi_tb where pk in (1,2,3,4, 5+7)", "false"},
        {"explain select * from select_base_one_multi_db_multi_tb where pk in (cast (7 as signed))", "false"},
        {"explain select * from select_base_one_multi_db_multi_tb where pk in (cast(integer_test as signed))", "false"},
        {"explain select * from select_base_one_multi_db_multi_tb where pk = (cast(integer_test as signed))", "false"},
        {"explain select * from select_base_one_multi_db_multi_tb where pk>3", "false"},
        {"explain select * from select_base_one_multi_db_multi_tb where integer_test>3 or pk in (1,7,87,8)", "false"},
        {"explain select * from select_base_one_multi_db_multi_tb where integer_test>3 and pk in (1,7,87,8)", "true"},
        {
            "explain select * from select_base_one_multi_db_multi_tb where integer_test>3 and (pk, varchar_test) in ((1,'a'),(7, 'b'), (87, 'e'),(8, 'e'))",
            "true"},
    };

    @Test
    public void testIfPruneWork() {
        int count = 0;
        String rex = "[\\s\\S]*isDynamicParam[\\s\\S]*";
        for (String[] row : dynamicInSqlTest) {
            System.out.println(count++);
            if (row[1].equalsIgnoreCase("true")) {
                DataValidator.explainAllResultMatchAssert(row[0], null, this.getPolardbxConnection(), rex);
            } else {
                DataValidator.explainAllResultNotMatchAssert(row[0], null, this.getPolardbxConnection(), rex);
            }
        }
    }
}
