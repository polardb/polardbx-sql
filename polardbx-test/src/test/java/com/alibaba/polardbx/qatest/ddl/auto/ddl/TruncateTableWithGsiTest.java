package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class TruncateTableWithGsiTest extends DDLBaseNewDBTestCase {

    @Test
    public void testTruncateTableWithUGsi() {
        String tableName = "truncate_table_with_ugsi_1";
        String indexName = "ugsi_idx_1";
        String indexHint = "/*+TDDL:cmd_extra(UNIQUE_GSI_WITH_PRIMARY_KEY=false)*/";

        String sql =
            String.format("create table %s (id int, name varchar(10), col int, primary key(id)) partition by key(id)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = indexHint + String.format("alter table %s add global unique index %s(name) partition by key(name)",
            tableName, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s values(1, 'a', 1), (2, 'b', 2), (3, 'c', 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("update %s set name = 'b', col = 2 where name = 'b'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("truncate table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s values(1, 'a', 1), (2, 'b', 2), (3, 'c', 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("update %s set name = 'b', col = 2 where name = 'b'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
