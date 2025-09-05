package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class AlterTableCollateTest extends DDLBaseNewDBTestCase {

    @Test
    public void testAlterTableCollate() {
        String sql1 = "drop table if exists test_collate";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table test_collate(a int, b varchar(100)) default charset utf8mb4";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 =
            "alter table test_collate modify column b varchar(100) COMMENT 'this is comment' COLLATE utf8_general_ci";
        JdbcUtil.executeSuccess(tddlConnection, sql1);
    }

    @Test
    public void testAlterTableCollateWithOmc() {
        String sql1 = "drop table if exists test_collate2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table test_collate2(a int, b varchar(100)) default charset utf8mb4";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 =
            "alter table test_collate2 modify column b varchar(100) COMMENT 'this is comment' COLLATE utf8_general_ci, algorithm=omc";
        JdbcUtil.executeSuccess(tddlConnection, sql1);
    }

    public boolean usingNewPartDb() {
        return true;
    }
}
