package com.alibaba.polardbx.qatest.ddl.sharding.gsi;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class InvisibleGsiTest extends DDLBaseNewDBTestCase {

    private static final String TABLE_NAME = "invisible_gsi_table";
    private static final String GSI_NAME = "g_i";
    private static final String UGSI_NAME = "ug_i";
    private static final String CGSI_NAME = "cg_i";

    private static final List<String> GSI_LIST = ImmutableList.of(
        GSI_NAME,
        UGSI_NAME,
        CGSI_NAME);

    @Before
    public void clearTableBefore() {
        dropTableWithGsi(TABLE_NAME, GSI_LIST);
    }

    @Test
    public void testCreateTableAndInvisibleGsi() throws Exception {

        //create invisible gsi
        final String createTable = MessageFormat.format("create table {0}"
                + "("
                + "id int,"
                + "name varchar(20),"
                + "global index {1} (`name`) dbpartition by hash(name) invisible,"
                + "unique global index {2} (`name`) dbpartition by hash(name) invisible,"
                + "clustered index {3} (`name`) dbpartition by hash(name) invisible"
                + ")"
                + "dbpartition by hash(id)",
            TABLE_NAME, GSI_NAME, UGSI_NAME, CGSI_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        for (int i = 0; i < 10; i++) {
            String sql = "insert into {0} values({1}, {2})";
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(sql, TABLE_NAME, i, i));
        }

        //not contains indexScan in plan
        String explainSql = MessageFormat.format(
            "explain select * from {0} where name=\"1\"",
            TABLE_NAME
        );
        assertNotContainStrInColResult(1, explainSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        //show
        String showSql = MessageFormat.format(
            "show create table {0}",
            TABLE_NAME
        );

        //change visibility
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            GSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            UGSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            CGSI_NAME
        ));

        for (int i = 10; i < 20; i++) {
            String sql = "insert into {0} values({1}, {2})";
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(sql, TABLE_NAME, i, i));
        }

        //contains indexScan in plan
        assertContainOneOfStrInColResult(1, explainSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        //show
        assertContainAllStrInColResult(2, showSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        gsiIntegrityCheck(GSI_NAME);
        gsiIntegrityCheck(CGSI_NAME);
        gsiIntegrityCheck(UGSI_NAME);

    }

    @Test
    public void testAlterTableAlterVisibility() throws Exception {
        //create visible gsi
        final String createTable = MessageFormat.format("create table {0}"
                + "("
                + "id int,"
                + "name varchar(20),"
                + "global index {1} (`name`) dbpartition by hash(name) visible,"
                + "unique global index {2} (`name`) dbpartition by hash(name) visible,"
                + "clustered index {3} (`name`) dbpartition by hash(name) visible"
                + ")"
                + "dbpartition by hash(id)",
            TABLE_NAME, GSI_NAME, UGSI_NAME, CGSI_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        for (int i = 0; i < 10; i++) {
            String sql = "insert into {0} values({1}, {2})";
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(sql, TABLE_NAME, i, i));
        }

        String explainSql = MessageFormat.format(
            "explain select * from {0} where name=\"1\"",
            TABLE_NAME
        );

        String showSql = MessageFormat.format(
            "show create table {0}",
            TABLE_NAME
        );

        //contains indexScan in plan
        assertContainOneOfStrInColResult(1, explainSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        //show
        assertContainAllStrInColResult(2, showSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        //change visibility
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} invisible",
            TABLE_NAME,
            GSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} invisible",
            TABLE_NAME,
            UGSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} invisible",
            TABLE_NAME,
            CGSI_NAME
        ));

        for (int i = 10; i < 20; i++) {
            String sql = "insert into {0} values({1}, {2})";
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(sql, TABLE_NAME, i, i));
        }

        //not contains indexScan in plan
        assertNotContainStrInColResult(1, explainSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));
        gsiIntegrityCheck(GSI_NAME);
        gsiIntegrityCheck(CGSI_NAME);
        gsiIntegrityCheck(UGSI_NAME);

        //change to visible
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            GSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            UGSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            CGSI_NAME
        ));

        for (int i = 100; i < 200; i++) {
            String sql = "insert into {0} values({1}, {2})";
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(sql, TABLE_NAME, i, i));
        }

        //contains indexScan in plan
        assertContainOneOfStrInColResult(1, explainSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        //show
        assertContainAllStrInColResult(2, showSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

    }

    @Test
    public void testCreateInvisibleGsi() throws Exception {
        //create invisible gsi
        final String createTable = MessageFormat.format("create table {0}"
            + "("
            + "id int,"
            + "name varchar(20)"
            + ")"
            + "dbpartition by hash(id)", TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        for (int i = 0; i < 10; i++) {
            String sql = "insert into {0} values({1}, {2})";
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(sql, TABLE_NAME, i, i));
        }

        //create invisible gsi
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "create global index {0} on {1} (name) dbpartition by hash(name) invisible",
            GSI_NAME,
            TABLE_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "create unique global index {0} on {1} (name) dbpartition by hash(name) invisible",
            UGSI_NAME,
            TABLE_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "create clustered index {0} on {1} (name) dbpartition by hash(name) invisible",
            CGSI_NAME,
            TABLE_NAME
        ));

        //not contains indexScan in plan
        String explainSql = MessageFormat.format(
            "explain select * from {0} where name=\"1\"",
            TABLE_NAME
        );
        assertNotContainStrInColResult(1, explainSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        //show
        String showSql = MessageFormat.format(
            "show create table {0}",
            TABLE_NAME
        );

        //change visibility
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            GSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            UGSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            CGSI_NAME
        ));

        for (int i = 10; i < 20; i++) {
            String sql = "insert into {0} values({1}, {2})";
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(sql, TABLE_NAME, i, i));
        }

        //contains indexScan in plan
        assertContainOneOfStrInColResult(1, explainSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        //show
        assertContainAllStrInColResult(2, showSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        gsiIntegrityCheck(GSI_NAME);
        gsiIntegrityCheck(CGSI_NAME);
        gsiIntegrityCheck(UGSI_NAME);
    }

    @Test
    public void testAlterTableAddInvisibleGsi() throws Exception {
        //create invisible gsi
        final String createTable = MessageFormat.format("create table {0}"
            + "("
            + "id int,"
            + "name varchar(20)"
            + ")"
            + "dbpartition by hash(id)", TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        for (int i = 0; i < 10; i++) {
            String sql = "insert into {0} values({1}, {2})";
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(sql, TABLE_NAME, i, i));
        }

        //create invisible gsi
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} add global index {1}(name) dbpartition by hash(name) invisible",
            TABLE_NAME,
            GSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} add global index {1}(name) dbpartition by hash(name) invisible",
            TABLE_NAME,
            CGSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} add global index {1}(name) dbpartition by hash(name) invisible",
            TABLE_NAME,
            UGSI_NAME
        ));

        //not contains indexScan in plan
        String explainSql = MessageFormat.format(
            "explain select * from {0} where name=\"1\"",
            TABLE_NAME
        );
        assertNotContainStrInColResult(1, explainSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        //show
        String showSql = MessageFormat.format(
            "show create table {0}",
            TABLE_NAME
        );

        //change visibility
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            GSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            UGSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            CGSI_NAME
        ));

        for (int i = 10; i < 20; i++) {
            String sql = "insert into {0} values({1}, {2})";
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(sql, TABLE_NAME, i, i));
        }

        //contains indexScan in plan
        assertContainOneOfStrInColResult(1, explainSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        //show
        assertContainAllStrInColResult(2, showSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        gsiIntegrityCheck(GSI_NAME);
        gsiIntegrityCheck(CGSI_NAME);
        gsiIntegrityCheck(UGSI_NAME);
    }

    @Test
    public void testCannotAlterLocalIndexVisiblity() {
        final String createTable = MessageFormat.format("create table {0}"
                + "("
                + "id int,"
                + "name varchar(20),"
                + "local index key_name(name),"
                + "global index {1} (`name`) dbpartition by hash(name) invisible,"
                + "unique global index {2} (`name`) dbpartition by hash(name) invisible,"
                + "clustered index {3} (`name`) dbpartition by hash(name) invisible"
                + ")"
                + "dbpartition by hash(id)",
            TABLE_NAME, GSI_NAME, UGSI_NAME, CGSI_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        //change visibility
        JdbcUtil.executeFaied(tddlConnection, MessageFormat.format(
            "alter table {0} alter index key_name invisible",
            TABLE_NAME
        ), "only global index's visibility can be altered");
    }

    @Test
    public void testForceIndexWhenInvisible() throws Exception {
        final String createTable = MessageFormat.format("create table {0}"
                + "("
                + "id int,"
                + "name varchar(20),"
                + "local index key_name(name),"
                + "global index {1} (`name`) dbpartition by hash(name) invisible,"
                + "unique global index {2} (`name`) dbpartition by hash(name) invisible,"
                + "clustered index {3} (`name`) dbpartition by hash(name) invisible"
                + ")"
                + "dbpartition by hash(id)",
            TABLE_NAME, GSI_NAME, UGSI_NAME, CGSI_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        //not contains indexScan in plan
        String explainSql = MessageFormat.format(
            "explain select name from {0} where name=\"1\"",
            TABLE_NAME
        );
        assertNotContainStrInColResult(1, explainSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

        //not contains indexsScan in plan only if force index
        String explainSqlForceIndex = MessageFormat.format(
            "explain select name from {0} where name=\"1\"",
            TABLE_NAME
        );
        assertNotContainStrInColResult(1, explainSqlForceIndex, ImmutableList.of(
                GSI_NAME,
                CGSI_NAME,
                UGSI_NAME
            )
        );

        //change visibility
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            GSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            UGSI_NAME
        ));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "alter table {0} alter index {1} visible",
            TABLE_NAME,
            CGSI_NAME
        ));

        //contains indexScan in plan
        assertContainOneOfStrInColResult(1, explainSql, ImmutableList.of(
            GSI_NAME,
            CGSI_NAME,
            UGSI_NAME
        ));

    }

    @After
    public void clearTable() {
        dropTableWithGsi(TABLE_NAME, GSI_LIST);
    }

    private void assertNotContainStrInColResult(int resultColIdx, String sql, List<String> strs) throws Exception {
        try (ResultSet resultSet = JdbcUtil.executeQuery(
            sql, tddlConnection
        )) {
            if (resultSet.next()) {
                String result = resultSet.getString(resultColIdx);
                strs.forEach(
                    str -> {
                        Assert.assertTrue(result != null && !result.toLowerCase().contains(str.toLowerCase()));
                    }
                );
            }
        }
    }

    private void assertNotContainStrInColResult2(int resultColIdx, String sql, List<String> strs, List<String> strs2)
        throws Exception {
        try (ResultSet resultSet = JdbcUtil.executeQuery(
            sql, tddlConnection
        )) {
            if (resultSet.next()) {
                String result = resultSet.getString(resultColIdx);
                for (int i = 0; i < strs.size(); i++) {
                    Assert.assertTrue(result != null &&
                        (!result.toLowerCase().contains(strs.get(i).toLowerCase()) || !result.toLowerCase()
                            .contains(strs2.get(i).toLowerCase()))
                    );
                }
            }
        }
    }

    private void assertContainOneOfStrInColResult(int resultColIdx, String sql, List<String> strs) throws Exception {
        try (ResultSet resultSet = JdbcUtil.executeQuery(
            sql, tddlConnection
        )) {
            if (resultSet.next()) {
                String result = resultSet.getString(resultColIdx);
                int sz =
                    strs.stream()
                        .filter(str -> result != null && result.toLowerCase().contains(str.toLowerCase()))
                        .collect(Collectors.toList()).size();

                Assert.assertTrue(sz > 0);
            }
        }
    }

    private void assertContainAllStrInColResult(int resultColIdx, String sql, List<String> strs) throws Exception {
        try (ResultSet resultSet = JdbcUtil.executeQuery(
            sql, tddlConnection
        )) {
            if (resultSet.next()) {
                String result = resultSet.getString(resultColIdx);
                strs.forEach(
                    str -> {
                        Assert.assertTrue(result != null && result.toLowerCase().contains(str.toLowerCase()));
                    }
                );
            }
        }
    }

    private void gsiIntegrityCheck(String index) throws Exception {
        final String createTable = JdbcUtil.showCreateTable(tddlConnection, index);
        final String tableName = createTable.substring("CREATE TABLE `".length(), createTable.indexOf("` ("));

        final String CHECK_HINT =
            "/*+TDDL: cmd_extra(GSI_CHECK_PARALLELISM=4, GSI_CHECK_BATCH_SIZE=1024, GSI_CHECK_SPEED_LIMITATION=-1)*/";
        final ResultSet rs = JdbcUtil
            .executeQuery(CHECK_HINT + "check global index " + tableName, tddlConnection);
        List<String> result = JdbcUtil.getStringResult(rs, false)
            .stream()
            .map(row -> row.get(row.size() - 1))
            .collect(Collectors.toList());
        System.out.println("Checker: " + result.get(result.size() - 1));
        Assert.assertTrue(result.get(result.size() - 1).contains("OK"));
    }

}
