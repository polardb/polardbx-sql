package com.alibaba.polardbx.qatest.ddl.sharding.repartition;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class RepartitionFailedTest extends DDLBaseNewDBTestCase {

    @Test
    public void testRepartitionFailed() {
        String sql = "drop table if exists tb123";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table tb123 (a int) dbpartition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table tb123 CHARACTER SET = utf8mb4, COLLATE = utf8mb4_unicode_ci single";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "does not support");

        sql = "alter table tb123 single CHARACTER SET = utf8mb4, COLLATE = utf8mb4_unicode_ci";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "does not support");

        sql = "alter table tb123 CHARACTER SET = utf8mb4, COLLATE = utf8mb4_unicode_ci broadcast";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "does not support");

        sql = "alter table tb123 broadcast CHARACTER SET = utf8mb4, COLLATE = utf8mb4_unicode_ci";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "does not support");

        sql = "alter table tb123 CHARACTER SET = utf8mb4, COLLATE = utf8mb4_unicode_ci dbpartition by hash(a)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "does not support");

        sql = "alter table tb123 dbpartition by hash(a) CHARACTER SET = utf8mb4, COLLATE = utf8mb4_unicode_ci";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "does not support");
    }

    @Test
    public void testRepartitionWithZeroAutoValue() {
        String sql = "drop table if exists tb1234";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table tb1234 (a int primary key auto_increment, b int) dbpartition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String sqlMode = JdbcUtil.getSqlMode(tddlConnection);
        try {
            sql = "set sql_mode = 'NO_AUTO_VALUE_ON_ZERO'";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "insert into tb1234 values(0,1),(1,2)";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        } finally {
            setSqlMode(sqlMode, tddlConnection);
        }

        sql = "alter table tb1234 dbpartition by hash(b)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Override
    public boolean usingNewPartDb() {
        return false;
    }
}
