package com.alibaba.polardbx.qatest.ddl.sharding.autoPartition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

/**
 * @version 1.0
 */
public class AutoPartitionCheckerTest extends AutoPartitionTestBase {

    private static final String TABLE_NAME = "auto_partition_checker";

    @Test
    public void testCheckHint() throws Exception {

        dropTableIfExists(tddlConnection, TABLE_NAME);

        final String createSql = "CREATE TABLE " + TABLE_NAME + " (\n"
            + "  `t` timestamp null default CURRENT_TIMESTAMP,\n"
            + "  `x` int default 3,\n"
            + "  `order_id` varchar(20) DEFAULT 'abc',\n"
            + "  `seller_id` varchar(20) DEFAULT 'vb'\n"
            + ") dbpartition by hash(x);";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        final String insertSql = "insert into " + TABLE_NAME + " (x, order_id) values (1, 'c'),(2, 'd'),(3, 'f');";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        String indexName = "check_gsi_i";
        final String addGsiSql =
            "/*+TDDL: cmd_extra(GSI_BACKFILL_USE_FASTCHECKER=false)*/alter table " + TABLE_NAME
                + " add clustered index `" + indexName + "` (x) dbpartition by hash(x) partitions 3;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, addGsiSql);

        checkGsi(tddlConnection, indexName);

        //将gsi某列设置为write_only
        updateColumnStatus(tddlDatabase1, indexName, ImmutableList.of("order_id"), 2);

        checkGsi(tddlConnection, "/*+TDDL: cmd_extra(GSI_BACKFILL_USE_FASTCHECKER=false)*/",
            indexName);

        checkGsi(tddlConnection, "/*+TDDL: cmd_extra(GSI_BACKFILL_USE_FASTCHECKER=true)*/",
            indexName);

        //dropTableIfExists(tddlConnection, TABLE_NAME);
    }

    private void updateColumnStatus(String schemaName, String tableName, List<String> columnNames, int status) {
        try {
            ResultSet rs = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
            ResultSetMetaData rsmd = rs.getMetaData();
            int beforeColumnCnt = rsmd.getColumnCount();

            for (String columnName : columnNames) {
                String updateStatus = String.format(
                    "/*+TDDL:node('__META_DB__')*/ update columns set status=%d where table_schema='%s' and table_name='%s' and column_name='%s'",
                    status, schemaName, tableName, columnName);
                JdbcUtil.executeUpdateSuccess(tddlConnection, updateStatus);
            }

            String updateTableVersion = String.format(
                "/*+TDDL:node('__META_DB__')*/ update tables set version=version+1 where table_schema='%s' and table_name='%s'",
                schemaName, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, updateTableVersion);
            String refresh = String.format(
                "/*+TDDL:node('__META_DB__')*/ update config_listener set op_version=op_version+1 where data_id = 'polardbx.meta.table.%s.%s'",
                schemaName, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, refresh);

            int cnt = 0;
            while (cnt < 60) {
                rs = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
                rsmd = rs.getMetaData();
                if (rsmd.getColumnCount() != beforeColumnCnt) {
                    return;
                }
                cnt++;
                if (cnt % 10 == 0) {
                    JdbcUtil.executeUpdateSuccess(tddlConnection, updateTableVersion);
                }
                Thread.sleep(1000);
                JdbcUtil.executeUpdateSuccess(tddlConnection, refresh);
                System.out.println(
                    "wait after update " + schemaName + " " + tableName + " " + beforeColumnCnt + " " + cnt);
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        Assert.fail("update column status failed");
    }
}
