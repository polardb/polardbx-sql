package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.SCHEDULED_JOBS;

public class MaterializedViewTest extends AsyncDDLBaseNewDBTestCase {

    private String tableName = "test_table_MaterializedViewTest";

    private Connection metaDBConn = getMetaConnection();

    @Test
    public void testCreateAndDropMaterializedView() throws SQLException {
        String createTableSql = "create table " + tableName + "(id int)";
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        int id = 1;
        String insertSql = "insert into " + tableName + " values(" + id + ")";
        JdbcUtil.executeSuccess(tddlConnection, insertSql);

        String mvName = tableName + "_mv";
        String createMVSql = "create materialized view " + mvName + " as select id from " + tableName;
        JdbcUtil.executeSuccess(tddlConnection, createMVSql);

        String querySql = "select id from " + mvName;
        ResultSet rs = JdbcUtil.executeQuery(querySql, tddlConnection);
        Assert.assertTrue(rs.next());
        int queryId = rs.getInt(1);
        Assert.assertTrue(id == queryId);

        String dropMVSql = "drop materialized view " + mvName;
        JdbcUtil.executeSuccess(tddlConnection, dropMVSql);
        JdbcUtil.dropTable(tddlConnection, tableName);

    }

    @Test
    public void testCreateRefreshMaterializedViewScheduledJob() throws SQLException {
        String createTableSql = "create table " + tableName + "(id int)";
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        String mvName = tableName + "_mv";
        String createMVSql = "create materialized view " + mvName + " as select id from " + tableName;
        JdbcUtil.executeSuccess(tddlConnection, createMVSql);

        String createScheduledJobSql = "CREATE SCHEDULE FOR REFRESH_MATERIALIZED_VIEW ON `" + tableName
            + "` CRON '0 0 12 1/5 * ?' TIMEZONE '+00:00'";
        JdbcUtil.executeSuccess(tddlConnection, createScheduledJobSql);

        String queryScheduledJobsSql = "select schedule_id from " + SCHEDULED_JOBS + " where  table_name= '"
            + tableName + "' and executor_type = '" + ScheduledJobExecutorType.REFRESH_MATERIALIZED_VIEW.name() + "'";
        //System.out.println(queryScheduledJobsSql);
        Statement statement = metaDBConn.createStatement();
        ResultSet rs = statement.executeQuery(queryScheduledJobsSql);
        Assert.assertTrue(rs.next());
        long scheduleId = rs.getLong(1);

        String dropScheduledJobSql = "DROP SCHEDULE " + scheduleId;
        JdbcUtil.executeSuccess(tddlConnection, dropScheduledJobSql);

        String dropMVSql = "drop materialized view " + mvName;
        JdbcUtil.executeSuccess(tddlConnection, dropMVSql);
        JdbcUtil.dropTable(tddlConnection, tableName);
    }
}
