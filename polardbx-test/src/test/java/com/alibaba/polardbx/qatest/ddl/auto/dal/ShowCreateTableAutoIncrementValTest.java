package com.alibaba.polardbx.qatest.ddl.auto.dal;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.truth.Truth.assertThat;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ShowCreateTableAutoIncrementValTest extends DDLBaseNewDBTestCase {

    private List<String> tbNames = ImmutableList.of(
        "new_seq_tb_partition",
        "new_seq_tb_single",
        "new_seq_tb_broadcast",
        "group_seq_tb_partition",
        "group_seq_tb_single",
        "group_seq_tb_broadcast",
        "tm_based_tb_partition",
        "tm_based_tb_single",
        "tm_based_tb_broadcast",
        "no_seq_tb_partition",
        "no_seq_tb_single",
        "no_seq_tb_broadcast",
        "implicit_tb_partition",
        "implicit_tb_single",
        "implicit_tb_broadcast",
        "group_seq_unit_partition",
        "group_seq_unit_single",
        "group_seq_unit_broadcast"
    );

    private List<String> tbSqls = ImmutableList.of(
        "create table new_seq_tb_partition("
            + "id int primary key auto_increment by new"
            + ") partition by hash(id) partitions 128",
        "create table new_seq_tb_single("
            + "id int primary key auto_increment by new"
            + ") single",
        "create table new_seq_tb_broadcast("
            + "id int primary key auto_increment"
            + ") broadcast",
        "create table group_seq_tb_partition("
            + "id int primary key auto_increment by group"
            + ") partition by hash(id) partitions 128",
        "create table group_seq_tb_single("
            + "id int primary key auto_increment by group"
            + ")single",
        "create table group_seq_tb_broadcast("
            + "id int primary key auto_increment by group"
            + ")broadcast",
        "create table tm_based_tb_partition("
            + "id bigint primary key auto_increment by time"
            + ") partition by hash(id) partitions 128",
        "create table tm_based_tb_single("
            + "id bigint primary key auto_increment by time"
            + ") single",
        "create table tm_based_tb_broadcast("
            + "id bigint primary key auto_increment by time"
            + ") broadcast",
        "create table no_seq_tb_partition("
            + "id bigint primary key"
            + ") partition by hash(id)",
        "create table no_seq_tb_single("
            + "id bigint primary key"
            + ") single",
        "create table no_seq_tb_broadcast("
            + "id bigint primary key"
            + ") broadcast",
        "create table implicit_tb_partition("
            + "id bigint"
            + ") partition by hash(id)",
        "create table implicit_tb_single("
            + "id bigint"
            + ") single",
        "create table implicit_tb_broadcast("
            + "id bigint"
            + ") broadcast",
        "create table group_seq_unit_partition("
            + "id int primary key auto_increment by group unit count 4 index 3"
            + ") partition by hash(id) partitions 128",
        "create table group_seq_unit_single("
            + "id int primary key auto_increment by group unit count 4 index 3"
            + ")single",
        "create table group_seq_unit_broadcast("
            + "id int primary key auto_increment by group unit count 4 index 3"
            + ")broadcast"
    );

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void clean() {
        for (String tbName : tbNames) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tbName);
        }
    }

    @Test
    public void testShowAutoIncrementNewSeq() {
        //prepare table
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(0));
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(1));
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(2));
        int autoIncrementAnswer = 1;
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            int randomNumber = random.nextInt(50) + 1;

            //partition table
            prepareData(tbNames.get(0), randomNumber);
            autoIncrementAnswer += randomNumber;
            String result = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "show full create table " + tbNames.get(0))) {
                assertThat(rs.next()).isTrue();
                result = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result != null
                && (result.contains("AUTO_INCREMENT = " + autoIncrementAnswer)) || result.contains(
                "auto_increment = " + autoIncrementAnswer)
            );

            //single table
            prepareData(tbNames.get(1), randomNumber);
            String result2 = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "show full create table " + tbNames.get(1))) {
                assertThat(rs.next()).isTrue();
                result2 = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result2 != null
                && (result2.contains("AUTO_INCREMENT = " + autoIncrementAnswer)) || result2.contains(
                "auto_increment = " + autoIncrementAnswer)
            );

            //single table
            prepareData(tbNames.get(2), randomNumber);
            String result3 = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show create table " + tbNames.get(2))) {
                assertThat(rs.next()).isTrue();
                result3 = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result3 != null
                && (result3.contains("AUTO_INCREMENT = " + autoIncrementAnswer)) || result3.contains(
                "auto_increment = " + autoIncrementAnswer)
            );
        }
    }

    @Test
    public void testShowAutoIncrementGroupSeqTmSeq() {
        //prepare table
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(3));
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(4));
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(5));
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(6));
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(7));
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(8));

        String autoIncrementPattern = "auto_increment = [0-9]+";
        Pattern regex = Pattern.compile(autoIncrementPattern, Pattern.CASE_INSENSITIVE);

        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            int randomNumber = random.nextInt(50) + 1;

            //partition table
            prepareData(tbNames.get(3), randomNumber);
            String result = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "show full create table " + tbNames.get(3))) {
                assertThat(rs.next()).isTrue();
                result = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result != null);
            Matcher matcher = regex.matcher(result);
            Assert.assertTrue(matcher.find());

            //single table
            prepareData(tbNames.get(4), randomNumber);
            String result2 = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "show full create table " + tbNames.get(4))) {
                assertThat(rs.next()).isTrue();
                result2 = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result2 != null);
            matcher = regex.matcher(result2);
            Assert.assertTrue(matcher.find());

            //broadcast
            prepareData(tbNames.get(5), randomNumber);
            String result3 = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show create table " + tbNames.get(5))) {
                assertThat(rs.next()).isTrue();
                result3 = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result3 != null);
            matcher = regex.matcher(result3);
            Assert.assertTrue(matcher.find());

            //partition
            prepareData(tbNames.get(6), randomNumber);
            String result4 = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show create table " + tbNames.get(6))) {
                assertThat(rs.next()).isTrue();
                result4 = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result4 != null);
            matcher = regex.matcher(result4);
            Assert.assertTrue(matcher.find());

            //single
            prepareData(tbNames.get(7), randomNumber);
            String result5 = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "show full create table " + tbNames.get(7))) {
                assertThat(rs.next()).isTrue();
                result5 = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result5 != null);
            matcher = regex.matcher(result5);
            Assert.assertTrue(matcher.find());

            //broadcast
            prepareData(tbNames.get(8), randomNumber);
            String result6 = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "show full create table " + tbNames.get(8))) {
                assertThat(rs.next()).isTrue();
                result6 = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result6 != null);
            matcher = regex.matcher(result6);
            Assert.assertTrue(matcher.find());
        }
    }

    /**
     * implicit 自增列不应该展示auto_increment
     */
    @Test
    public void testShowNoSequence() {
        //prepare table
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(9));
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(10));
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(11));
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(12));
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(13));
        JdbcUtil.executeUpdateSuccess(tddlConnection, tbSqls.get(14));

        String autoIncrementPattern = "auto_increment = [0-9]*";
        Pattern regex = Pattern.compile(autoIncrementPattern, Pattern.CASE_INSENSITIVE);
        int idx = 0;
        Random random = new Random();
        for (int i = 0; i < 5; i++) {
            int randomNumber = random.nextInt(50) + 1;

            //partition table
            prepareData2(tbNames.get(9), randomNumber, idx);
            String result = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "show full create table " + tbNames.get(9))) {
                assertThat(rs.next()).isTrue();
                result = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result != null);
            Matcher matcher = regex.matcher(result);
            Assert.assertTrue(!matcher.find());

            //single table
            prepareData2(tbNames.get(10), randomNumber, idx);
            String result2 = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "show create table " + tbNames.get(10))) {
                assertThat(rs.next()).isTrue();
                result2 = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result2 != null);
            matcher = regex.matcher(result2);
            Assert.assertTrue(!matcher.find());

            //broadcast table
            prepareData2(tbNames.get(11), randomNumber, idx);
            String result3 = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "show full create table " + tbNames.get(11))) {
                assertThat(rs.next()).isTrue();
                result3 = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result3 != null);
            matcher = regex.matcher(result3);
            Assert.assertTrue(!matcher.find());

            //partition table
            prepareData2(tbNames.get(12), randomNumber, idx);
            result = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "show full create table " + tbNames.get(12))) {
                assertThat(rs.next()).isTrue();
                result = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result != null);
            matcher = regex.matcher(result);
            Assert.assertTrue(!matcher.find());

            //single table
            prepareData2(tbNames.get(13), randomNumber, idx);
            result2 = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "show create table " + tbNames.get(13))) {
                assertThat(rs.next()).isTrue();
                result2 = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result2 != null);
            matcher = regex.matcher(result2);
            Assert.assertTrue(!matcher.find());

            //broadcast table
            prepareData2(tbNames.get(14), randomNumber, idx);
            result3 = null;
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "show full create table " + tbNames.get(14))) {
                assertThat(rs.next()).isTrue();
                result3 = rs.getString("Create Table");
            } catch (SQLException ignore) {
            }

            Assert.assertTrue(result3 != null);
            matcher = regex.matcher(result3);
            Assert.assertTrue(!matcher.find());

            idx += randomNumber;
        }

    }

    /**
     * 空表、sequence还未被使用过，不应该展示auto_increment
     */
    @Test
    public void testShowNoSequence2() {
        //prepare table
        for (String tbSql : tbSqls) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, tbSql);
        }

        Random random = new Random();
        //partition table

        for (int i = 0; i < 18; i++) {
            int randomNumber = random.nextInt(50) + 1;
            if (tbNames.get(i).contains("tm_based_tb")) {
                //should not reveal
                Assert.assertTrue(findAutoIncrementPattern(tbNames.get(i), tddlConnection, true));
                Assert.assertTrue(findAutoIncrementPattern(tbNames.get(i), tddlConnection, false));
            } else {
                //should not reveal
                Assert.assertTrue(!findAutoIncrementPattern(tbNames.get(i), tddlConnection, true));
                Assert.assertTrue(!findAutoIncrementPattern(tbNames.get(i), tddlConnection, false));
            }

            if (tbNames.get(i).contains("no_seq")) {
                prepareData3(tbNames.get(i), randomNumber);
            } else {
                prepareData(tbNames.get(i), randomNumber);
            }
            if (tbNames.get(i).contains("implicit_tb") || tbNames.get(i).contains("no_seq_tb")) {
                //should not reveal
                Assert.assertTrue(!findAutoIncrementPattern(tbNames.get(i), tddlConnection, true));
                Assert.assertTrue(!findAutoIncrementPattern(tbNames.get(i), tddlConnection, false));
            } else {
                //should reveal
                Assert.assertTrue(findAutoIncrementPattern(tbNames.get(i), tddlConnection, true));
                Assert.assertTrue(findAutoIncrementPattern(tbNames.get(i), tddlConnection, false));
            }

            if (!tbNames.get(i).contains("no_seq")) {
                cleanData(tbNames.get(i));
            }
            if (tbNames.get(i).contains("tm_based_tb")) {
                //should reveal
                Assert.assertTrue(findAutoIncrementPattern(tbNames.get(i), tddlConnection, true));
                Assert.assertTrue(findAutoIncrementPattern(tbNames.get(i), tddlConnection, false));
            } else {
                //should not reveal
                Assert.assertTrue(!findAutoIncrementPattern(tbNames.get(i), tddlConnection, true));
                Assert.assertTrue(!findAutoIncrementPattern(tbNames.get(i), tddlConnection, false));
            }

            //insert and reveal again
            if (!tbNames.get(i).contains("no_seq")) {
                prepareData(tbNames.get(i), 1);
            }

            if (tbNames.get(i).contains("implicit_tb") || tbNames.get(i).contains("no_seq_tb")) {
                //should not reveal
                Assert.assertTrue(!findAutoIncrementPattern(tbNames.get(i), tddlConnection, true));
                Assert.assertTrue(!findAutoIncrementPattern(tbNames.get(i), tddlConnection, false));
            } else {
                //should reveal
                Assert.assertTrue(findAutoIncrementPattern(tbNames.get(i), tddlConnection, true));
                Assert.assertTrue(findAutoIncrementPattern(tbNames.get(i), tddlConnection, false));
            }

        }
    }

    boolean findAutoIncrementPattern(String tableName, Connection connection, boolean full) {
        String autoIncrementPattern = "auto_increment = [0-9]+";
        Pattern regex = Pattern.compile(autoIncrementPattern, Pattern.CASE_INSENSITIVE);
        final String showSql = full ? "show full create table " : "show create table ";
        String result = null;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
            showSql + tableName)) {
            assertThat(rs.next()).isTrue();
            result = rs.getString("Create Table");
        } catch (SQLException ignore) {
        }

        Assert.assertTrue(result != null);
        Matcher matcher = regex.matcher(result);
        return matcher.find();
    }

    void prepareData(String tbName, int rowCount) {
        final String insertSql = "insert into %s values(null)";
        for (int i = 0; i < rowCount; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, tbName));
        }
    }

    void prepareData3(String tbName, int rowCount) {
        final String insertSql = "insert into %s values(%s)";
        for (int i = 0; i < rowCount; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, tbName, i));
        }
    }

    void prepareData2(String tbName, int rowCount, int begin) {
        final String insertSql = "insert into %s values(%d)";
        for (int i = 0; i < rowCount; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, tbName, begin + i));
        }
    }

    void cleanData(String tbName) {
        final String truncateSql = "truncate table %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(truncateSql, tbName));
        //reset sequence
        String sequenceName = SequenceAttribute.AUTO_SEQ_PREFIX + tbName;
        final String resetSql = "alter sequence %s start with 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(resetSql, sequenceName));
    }

}
