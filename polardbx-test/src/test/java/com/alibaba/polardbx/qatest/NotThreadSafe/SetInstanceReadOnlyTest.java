package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.DirectConnectionBaseTestCase;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class SetInstanceReadOnlyTest extends DirectConnectionBaseTestCase {

    private static final Logger logger = LoggerFactory.getLogger(SetInstanceReadOnlyTest.class);

    private final String DBA_USER_NAME = "test_gdn_dba_user";
    private final String NORMAL_USER_NAME = "test_gdn_normal_user";
    private final String GOD_USER_NAME = "test_gdn_god_user";
    // 测试一个不会影响实例的变量
    private final String VAR_NAME = "INSTANCE_READ_ONLY";
    private final String DB_NAME = "instance_readonly_test";
    private final String SET_GLOBAL_VAR_SQL_TEMPLATE = "set global %s = %s;";
    private final String GRANT_ALL_PRIVILEGE_FOR_USER =
        "grant all on *.* to '%s'@'%%';";

    private final String SET_USER_SUPER_ACCOUNT = "update metadb.user_priv set account_type = 2 where user_name = '%s'";
    private final String SET_USER_GOD_ACCOUNT = "update metadb.user_priv set account_type = 5 where user_name = '%s'";
    private final String DROP_TEST_DB_IF_EXIST = "drop database if exists instance_readonly_test";
    private final String CREATE_TEST_DB = "create database instance_readonly_test";
    private final String CREATE_TABLE = "create table if not exists t1(name varchar(20)) dbpartition by hash(name)";
    private final String CREATE_TABLE2 = "create table if not exists t2(name varchar(20)) dbpartition by hash(name)";
    private final String DROP_TABLE = "drop table t1;";
    private final String DROP_TABLE2 = "drop table t2;";
    private final String INSERT_DML = "insert into t1(name) values('%s');";
    private final String READ_ONLY_ERROR =
        "server is running with the instance-read-only option so it cannot execute this statement";

    private final String SET_SUPER_WRITE = "set super_write=true;";
    private final String POLARDBX_PASSWD = "123456";

    private final String DROP_USER = "drop user '%s'@'%%';";
    private final String CREATE_USER = "CREATE USER '%s'@'%%' IDENTIFIED BY '123456';";

    private final String polardbxPort = PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PORT);
    private final String SERVER_1 = PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_ADDRESS);

    private void dropAccountsIgnoreError(String userName) {
        try {
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_USER, userName));
        } catch (Exception e) {
            logger.error("drop user : " + userName + " error!", e);
        }
    }

    private void createUser(String userName) {
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_USER, userName));
    }

    private Connection getConn(String server, String user, String password) {
        return getPolardbxDirectConnection(server, user, password, polardbxPort);

    }

    /**
     * 重试10s，上面的set 操作可能有延迟入库的情况， 下面可能有并发问题，所以这里需要重试确认参数变更成功
     */
    private void waitForVariable(Connection conn, String variableName, String val) {
        AtomicInteger counter = new AtomicInteger(10);
        // 重试10s，上面的set 操作可能有延迟入库的情况， 下面可能有并发问题，所以这里需要重试确认参数变更成功
        while (true) {
            ResultSet rs = JdbcUtil.executeQuery(String.format("show variables like '%%%s%%';", variableName), conn);
            try {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(val, rs.getString(2));
                break;
            } catch (Throwable e) {
                if (counter.decrementAndGet() > 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (Exception ex) {
                    }
                } else {
                    Assert.fail(String.format("%s is not set to true", variableName));
                }
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                    }
                }
            }
        }
    }

    /**
     * 设置instance_read_only 后，只有root可以写入
     * super_write 只支持session级别设置
     * 其他账号类型可以通过设置 super_write 后可以写入
     */
    @Test
    public void testSetGlobalPriv() {

        // 准备账号
        dropAccountsIgnoreError(DBA_USER_NAME);
        dropAccountsIgnoreError(NORMAL_USER_NAME);
        dropAccountsIgnoreError(GOD_USER_NAME);
        createUser(DBA_USER_NAME);
        createUser(NORMAL_USER_NAME);
        createUser(GOD_USER_NAME);
        JdbcUtil.executeSuccess(tddlConnection, String.format(GRANT_ALL_PRIVILEGE_FOR_USER, DBA_USER_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(GRANT_ALL_PRIVILEGE_FOR_USER, NORMAL_USER_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(GRANT_ALL_PRIVILEGE_FOR_USER, GOD_USER_NAME));
        // 设置为高权限
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_USER_SUPER_ACCOUNT, DBA_USER_NAME));
        // 设置为root用户
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_USER_GOD_ACCOUNT, GOD_USER_NAME));

        // 准备库表
        JdbcUtil.executeSuccess(tddlConnection, DROP_TEST_DB_IF_EXIST);
        JdbcUtil.executeSuccess(tddlConnection, CREATE_TEST_DB);
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        // 禁写
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_GLOBAL_VAR_SQL_TEMPLATE, VAR_NAME, true));

        // 高权限账号测试
        try (Connection tmpUserConn = getConn(SERVER_1, DBA_USER_NAME, POLARDBX_PASSWD)) {

            // 重试10s，上面的set 操作可能有延迟入库的情况， 下面可能有并发问题，所以这里需要重试确认参数变更成功
            waitForVariable(tmpUserConn, "INSTANCE_READ_ONLY", "true");
            // 高权限账号 没有super_write 无法执行ddl
            JdbcUtil.useDb(tmpUserConn, DB_NAME);

            JdbcUtil.executeFailed(tmpUserConn, CREATE_TABLE, READ_ONLY_ERROR);

            // root账号可以执行create table
            JdbcUtil.executeSuccess(tddlConnection, CREATE_TABLE);
            // root账号可以正常写入
            JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DML, RandomStringUtils.random(10)));
            // 高权限账号 没有设置 super_write报错
            JdbcUtil.executeFailed(tmpUserConn, String.format(INSERT_DML, RandomStringUtils.random(10)),
                READ_ONLY_ERROR);

            JdbcUtil.executeSuccess(tmpUserConn, SET_SUPER_WRITE);
            // 高权限账号 设置super_write 后，可以正常写入
            JdbcUtil.executeSuccess(tmpUserConn, String.format(INSERT_DML, RandomStringUtils.random(10)));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // 普通账号
        try (Connection tmpUserConn = getConn(SERVER_1, NORMAL_USER_NAME, POLARDBX_PASSWD)) {

            // 普通账号 没有super_write 无法执行ddl
            JdbcUtil.useDb(tmpUserConn, DB_NAME);

            JdbcUtil.executeFailed(tmpUserConn, DROP_TABLE, READ_ONLY_ERROR);

            // 普通账号 没有设置 super_write报错
            JdbcUtil.executeFailed(tmpUserConn, String.format(INSERT_DML, RandomStringUtils.random(10)),
                READ_ONLY_ERROR);

            JdbcUtil.executeSuccess(tmpUserConn, SET_SUPER_WRITE);
            // 普通账号 设置super_write 后，可以正常写入
            JdbcUtil.executeSuccess(tmpUserConn, String.format(INSERT_DML, RandomStringUtils.random(10)));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // 手动GOD账号
        try (Connection tmpUserConn = getConn(SERVER_1, GOD_USER_NAME, POLARDBX_PASSWD)) {

            // GOD账号 没有super_write 无法执行ddl
            JdbcUtil.useDb(tmpUserConn, DB_NAME);

            JdbcUtil.executeSuccess(tmpUserConn, CREATE_TABLE2);
            // GOD账号 设置super_write 后，可以正常写入
            JdbcUtil.executeSuccess(tmpUserConn, String.format(INSERT_DML, RandomStringUtils.random(10)));
            JdbcUtil.executeSuccess(tmpUserConn, DROP_TABLE2);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void afterTest() {
        JdbcUtil.setGlobal(tddlConnection, String.format(SET_GLOBAL_VAR_SQL_TEMPLATE, VAR_NAME, false));
        dropAccountsIgnoreError(DBA_USER_NAME);
        dropAccountsIgnoreError(NORMAL_USER_NAME);
        dropAccountsIgnoreError(GOD_USER_NAME);
    }
}
