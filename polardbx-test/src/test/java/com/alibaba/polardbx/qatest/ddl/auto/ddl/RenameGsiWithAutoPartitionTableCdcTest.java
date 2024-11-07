package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class RenameGsiWithAutoPartitionTableCdcTest extends DDLBaseNewDBTestCase {

    @Test
    public void testRenamePhyTable() {
        // 预期 cdc 实验室不报错
        String sql = "drop table if exists auto_table_rename_index";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table auto_table_rename_index(a int, b int)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table auto_table_rename_index add index idx_a(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table auto_table_rename_index add index idx_b(b)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table auto_table_rename_index rename index idx_a to idx_a_new";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table auto_table_rename_index rename index idx_b to idx_b_new";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table auto_table_rename_index drop index idx_a_new";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table auto_table_rename_index drop index idx_b_new";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testRenamePhyTable2() {
        // 预期 cdc 实验室不报错
        String sql = "drop table if exists auto_table_rename_index2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table auto_table_rename_index2(a int, b int, index idx_a(a), index idx_b(b))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table auto_table_rename_index2 rename index idx_a to idx_a_new";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table auto_table_rename_index2 rename index idx_b to idx_b_new";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table auto_table_rename_index2 drop index idx_a_new";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table auto_table_rename_index2 drop index idx_b_new";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
