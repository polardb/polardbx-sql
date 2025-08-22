package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class ErrorMsgTest extends DDLBaseNewDBTestCase {

    @Test
    public void testOnlineModifyColumnErrorMsg() {
        String tableName = "omc_err_msg_t1";
        String sql = String.format("create table %s (a int primary key, b int, c int) partition by key(a)", tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b bigint unique, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Do not support modify the column with unique key or primary key when using online modify column");

        sql = String.format("alter table %s modify column b bigint after b, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Do not support insert after the same column");

        sql = String.format("alter table %s modify column a bigint auto_increment, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Missing sequence for auto increment column");

        sql = String.format("alter table %s modify column d bigint, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Modify unknown column");

        sql = String.format("alter table %s modify column b int, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "It seems that you do not alter column type, try to turn off ALGORITHM=OMC for better practice");

        sql = String.format("alter table %s modify column b bigint GENERATED ALWAYS AS (a) virtual, algorithm=omc",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "can not be modified to a generated column");

        // with add column
        sql = String.format("alter table %s add column d bigint, drop column d, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Can't DROP 'd'; check that column/key exists");

        sql = String.format("alter table %s modify column b bigint,add column d int,add column d int, algorithm=omc;",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate column name 'd'");

        sql = String.format("alter table %s modify column b bigint, add column c int, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Do not support add the column which is already existed when using online modify column");

        sql = String.format("alter table %s modify column b bigint, add column d int not null, algorithm=omc;",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Do not support add the column which is not null and have no default value when using online modify column");

        sql = String.format("alter table %s modify column b bigint, add column d int auto_increment, algorithm=omc;",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Missing sequence for auto increment column");

        sql = String.format("alter table %s modify column b bigint, add column d int unique, algorithm=omc;",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Do not support add the column with unique key or primary key when using online modify column");

        sql = String.format(
            "alter table %s modify column b bigint, add column d int GENERATED ALWAYS AS (b) virtual, algorithm=omc;",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Do not support add the column with generated expr when using online modify column");

        // with drop column
        sql = String.format("alter table %s modify column b bigint, drop column b, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Unknown column");

        sql = String.format("alter table %s modify column b bigint, drop column d, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Can't DROP 'd'; check that column/key exists");

        sql = String.format("alter table %s modify column b bigint, drop column a, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Do not support drop primary key");

        sql = String.format("alter table %s modify column b bigint, add index idx1(b), algorithm=omc",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Online column modify only supports modify column or change column");
    }

    @Test
    public void testOnlineModifyColumnErrorMsg2() {
        String tableName = "omc_err_msg_t2";
        String sql = String.format("create table %s (a int primary key, b int, c int) partition by key(a)", tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column b d bigint unique, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Do not support change the column with unique key or primary key when using online modify column");

        sql = String.format("alter table %s change column b d bigint after b, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Do not support insert after the same column");

        sql = String.format("alter table %s change column a a bigint auto_increment, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Missing sequence for auto increment column");

        sql = String.format("alter table %s change column d d bigint, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Modify unknown column");

        sql = String.format("alter table %s change column b b int, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "It seems that you do not alter column type, try to turn off ALGORITHM=OMC for better practice");

        sql = String.format("alter table %s change column b d bigint GENERATED ALWAYS AS (a) virtual, algorithm=omc",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "can not be changed to a generated column");

        sql = String.format("alter table %s change column a d bigint, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Online modify primary key name is not supported");
    }

    @Test
    public void testOnlineModifyColumnErrorMsg3() {
        String tableName = "omc_err_msg_t3";
        String sql = String.format("create table %s (a int primary key, b int, c int, d int GENERATED ALWAYS AS (a + b)) partition by key(a)", tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column d d bigint, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Can not change generated column");

        sql = String.format("alter table %s modify column d bigint, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Can not modify generated column");

        sql = String.format("alter table %s change column b b bigint, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "referenced by a generated column");

        sql = String.format("alter table %s modify column b bigint, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "referenced by a generated column");
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
