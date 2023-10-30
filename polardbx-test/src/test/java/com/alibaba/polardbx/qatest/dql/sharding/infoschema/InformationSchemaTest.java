package com.alibaba.polardbx.qatest.dql.sharding.infoschema;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;

public class InformationSchemaTest extends DDLBaseNewDBTestCase {

    static private final String case_tb = "cASE_tB";

    private static final String CREATE_TABLE_FORMAT_MIX_CASE = String.format("CREATE TABLE `%s` (\n"
        + "\t`pk` bigint(11) NOT NULL,\n"
        + "\t`INTEGER_teST` int(11) DEFAULT NULL,\n"
        + "\t`varchaR_TEst` varchar(255) DEFAULT NULL,\n"
        + "\tPRIMARY KEY (`pk`),\n"
        + "\tKEY `i_0` (`INTEGER_teST`),\n"
        + "\tKEY `I_1` (`varchaR_TEst`),\n"
        + "\tUNIQUE KEY `Uu_0` (`varchaR_TEst`,`INTEGER_teST`)\n"
        + ")", case_tb);

    private static final String INFO_SCHEMA =
        "select schema_name from information_schema.schemata";
    private static final String INFO_TABLES =
        "select table_schema, table_name from information_schema.tables where table_schema = '%s' and table_name = '%s'";

    private static final String INFO_COLUMNS =
        "select table_schema, table_name, column_name from information_schema.columns where table_schema = '%s' and table_name = '%s'";
    private static final String INFO_STATISTICS =
        "select table_schema, table_name, index_schema, index_name, column_name from information_schema.statistics where table_schema = '%s' and table_name = '%s'";

    private static final String INFO_TB_CONSTRAINTS =
        "select CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME , CONSTRAINT_TYPE"
            + " from information_schema.table_constraints where table_schema = '%s' and table_name = '%s'";

    @Override
    protected Connection getTddlConnection1() {
        if (tddlConnection == null) {
            String database1 = getTestDBName("");
            String myDatabase1 = database1;
            this.tddlConnection = createTddlDb(database1);
            this.mysqlConnection = createMysqlDb(myDatabase1);
            this.tddlDatabase1 = database1;
            this.mysqlDatabase1 = myDatabase1;
            this.infomationSchemaDbConnection = getMysqlConnection("information_schema");
        }
        return tddlConnection;
    }

    @Override
    protected String getTestDBName(String schemaPrefix) {
        String database1 = schemaPrefix + Math.abs(Thread.currentThread().getName().hashCode());
        database1 = database1 + "_cASE_DB";
        return database1;
    }

    @Before
    public void prepareVariable() {
        JdbcUtil.executeUpdateSuccess(getPolardbxConnection(), "set global ENABLE_LOWER_CASE_TABLE_NAMES=true");
    }

    @After
    public void clearVariable() {
        JdbcUtil.executeUpdateSuccess(getPolardbxConnection(), "set global ENABLE_LOWER_CASE_TABLE_NAMES=false");
        cleanDataBase();
    }

    @Test
    public void testCase() throws SQLException, InterruptedException {
        dropTableIfExists(case_tb);
        dropTableIfExistsInMySql(case_tb);

        String partitionDef = " dbpartition by hash(`pk`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TABLE_FORMAT_MIX_CASE + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, CREATE_TABLE_FORMAT_MIX_CASE);

        String db;
        String tb;
        String sql;

        // wait 2 second for set global
        Thread.sleep(2000L);
        // show database and information_schema.schemata
        sql = "show databases";
        ResultSet rs = JdbcUtil.executeQuery(sql, mysqlConnection);
        Optional<String> goal =
            JdbcUtil.getAllResult(rs).stream().filter(x -> tddlDatabase1.equalsIgnoreCase((String) x.get(0)))
                .map(x -> (String) x.get(0)).findFirst();
        assertWithMessage("can't find database " + tddlDatabase1 + "in MySQL")
            .that(goal.isPresent()).isTrue();
        rs.close();
        db = goal.get();

        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        boolean found =
            JdbcUtil.getAllResult(rs).stream().anyMatch(x -> db.equals((String) x.get(0)));
        assertWithMessage("find database " + db + " in 'show databases'")
            .that(found).isTrue();
        rs.close();
        sql = INFO_SCHEMA;
        checkInfoSchema(db, sql, mysqlConnection);
        checkInfoSchema(db, sql, tddlConnection);

        // show tables and information_schema.tables
        sql = "show tables";
        rs = JdbcUtil.executeQuery(sql, mysqlConnection);
        goal =
            JdbcUtil.getAllResult(rs).stream().filter(x -> case_tb.equalsIgnoreCase((String) x.get(0)))
                .map(x -> (String) x.get(0)).findFirst();
        assertWithMessage("find table " + case_tb + " in MySQL")
            .that(goal.isPresent()).isTrue();
        rs.close();
        tb = goal.get();

        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        found =
            JdbcUtil.getAllResult(rs).stream().anyMatch(x -> tb.equals(x.get(0)));
        assertWithMessage("find table " + tb + " in 'show table'")
            .that(found).isTrue();
        rs.close();
        sql = String.format(INFO_TABLES, db, tb);
        checkInfoTables(db, tb, sql, mysqlConnection);
        checkInfoTables(db, tb, sql, tddlConnection);

        // show columns and information_schema.columns
        sql = "show columns from " + tb;
        rs = JdbcUtil.executeQuery(sql, mysqlConnection);
        Set<String> columns =
            JdbcUtil.getAllResult(rs).stream().map(x -> (String) x.get(0)).collect(Collectors.toSet());
        rs.close();

        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        found =
            JdbcUtil.getAllResult(rs).stream().allMatch(x -> columns.contains((String) x.get(0)));
        assertWithMessage("find all columns in 'show columns'")
            .that(found).isTrue();
        rs.close();
        sql = String.format(INFO_COLUMNS, db, tb);
        checkInfoColumns(db, tb, sql, mysqlConnection, columns);
        checkInfoColumns(db, tb, sql, tddlConnection, columns);

        // show index and information_schema.statistics
        sql = "show index from " + tb;
        rs = JdbcUtil.executeQuery(sql, mysqlConnection);
        Set<String> indexes =
            JdbcUtil.getAllResult(rs).stream().map(x -> (x.get(2) + (String) x.get(4)))
                .collect(Collectors.toSet());
        rs.close();

        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        found =
            JdbcUtil.getAllResult(rs).stream().allMatch(x -> indexes.contains(((String) x.get(2) + (String) x.get(4))));
        assertWithMessage("find all columns in 'show index'")
            .that(found).isTrue();
        rs.close();

        sql = String.format(INFO_STATISTICS, db, tb);
        checkInfoStatistics(db, tb, sql, mysqlConnection, indexes);
        checkInfoStatistics(db, tb, sql, tddlConnection, indexes);

        // check table_constraints
        sql = String.format(INFO_TB_CONSTRAINTS, db, tb);
        DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    private void checkInfoSchema(String db, String sql, Connection conn) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);
        boolean found =
            JdbcUtil.getAllResult(rs).stream().anyMatch(x -> db.equals(x.get(0)));
        assertWithMessage("find database " + db + " in 'information_schema.schemata'")
            .that(found).isTrue();
        rs.close();
    }

    private void checkInfoTables(String db, String tb, String sql, Connection conn) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);
        boolean found =
            JdbcUtil.getAllResult(rs).stream().anyMatch(x -> db.equals(x.get(0)) && tb.equals(x.get(1)));
        assertWithMessage("find table " + tb + " in 'information_schema.tables'")
            .that(found).isTrue();
        rs.close();
    }

    private void checkInfoColumns(String db, String tb, String sql, Connection conn, Set<String> columns)
        throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);
        boolean found = JdbcUtil.getAllResult(rs).stream()
            .allMatch(x -> db.equals(x.get(0)) && tb.equals(x.get(1)) && columns.contains((String) x.get(2)));
        assertWithMessage("find all columns in 'information_schema.columns'")
            .that(found).isTrue();
        rs.close();
    }

    private void checkInfoStatistics(String db, String tb, String sql, Connection conn, Set<String> indexes)
        throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);
        boolean found = JdbcUtil.getAllResult(rs).stream()
            .allMatch(x -> db.equals(x.get(0)) && tb.equals(x.get(1)) && db.equals(x.get(2)) && indexes.contains(
                (x.get(3) + (String) x.get(4))));
        assertWithMessage("find all columns in 'information_schema.statistics'")
            .that(found).isTrue();
        rs.close();
    }
}
